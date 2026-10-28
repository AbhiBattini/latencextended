"""Per-message processing shared by the live tracker and the offline report.

The live receive path and `analysis.report` both feed every message through `Engine.process`, so books, the merged
view, the touch race and every latency statistic are computed by one piece of code. Statistics go to a `sink` with two
methods, `add(metric, key, value)` and `count(metric, key, n=1)`.
"""

from __future__ import annotations

from collections import OrderedDict

from .book import MergedBook, OrderBook, TouchRace, touch_from_levels
from .feeds import BIN, V1_D1, V1_FULL, V1_PRICE, V1_TRADES, V2

AGE_SAMPLE_NS = 100_000_000  # merged-touch age is sampled on a 100 ms receive-time grid
HOLD_WINDOW_NS = 10_000_000_000
HOLD_LEVELS = 25
FULL_FOR_MERGE = "ext_full"
D1_FOR_MERGE = ("ext_d1_a", "ext_d1_b")
HOLD_PAIR = ("ext_full", "ext_rpc.full")


def label_for(feed: str, kind: str, msg) -> tuple[str, str | None]:
    """(label, role) for a message. Roles: d1, full, trades, price, bin, None (ack / control)."""
    if kind == V2:
        t = msg.get("type") if isinstance(msg, dict) else None
        if not t:
            return feed, None
        t = t.upper()
        if t.startswith("ORDERBOOK"):
            d = (msg.get("data") or {}).get("d")
            role = "d1" if str(d) == "1" else "full"
        elif t.startswith("TRADE"):
            role = "trades"
        else:
            role = t.lower()
        return (feed if feed.endswith("_d1") else f"{feed}.{role}"), role
    return feed, {V1_D1: "d1", V1_FULL: "full", V1_TRADES: "trades", V1_PRICE: "price", BIN: "bin"}.get(kind)


class Engine:
    def __init__(self, markets, sink, race_window_s: float = 5.0, compute_hold: bool = False) -> None:
        self.markets = list(markets)
        self.sink = sink
        self.race_window_s = race_window_s
        self.compute_hold = compute_hold
        self.reset()

    def reset(self) -> None:
        """Forget all connection state (new process / new archive segment). Pending race states are finalised."""
        if getattr(self, "race", None):
            for r in self.race.values():
                r.expire(None)
        self.books: dict[tuple, OrderBook] = {}
        self.full = {m: OrderBook() for m in self.markets}
        self.merged = {m: MergedBook(m, self.full[m]) for m in self.markets}
        if not getattr(self, "race", None):
            self.race = {m: TouchRace(m, self.race_window_s) for m in self.markets}
        self.conn: dict[tuple, object] = {}
        self.age_next: dict[str, int] = {}
        self.hold: dict[tuple, OrderedDict] = {}

    def _book(self, label: str, market: str) -> OrderBook:
        if label == FULL_FOR_MERGE and market in self.full:
            return self.full[market]
        b = self.books.get((label, market))
        if b is None:
            b = self.books[(label, market)] = OrderBook()
        return b

    def _on_new_conn(self, feed: str, market) -> None:
        for (label, mk), b in self.books.items():
            if mk == market and (label == feed or label.startswith(feed + ".")):
                b.reset()
        if feed == FULL_FOR_MERGE and market in self.full:
            self.full[market].reset()

    def finish(self) -> None:
        for r in self.race.values():
            r.expire(None)

    def process(self, feed: str, kind: str, market, conn_id, wall_ns: int, msg, excluded: bool, backfill: bool = False):
        """Returns (market, label) of a data message, or None for acks / unparseable messages."""
        ck = (feed, market)
        if self.conn.get(ck) != conn_id:
            self.conn[ck] = conn_id
            self._on_new_conn(feed, market)
        if not isinstance(msg, dict):
            return None
        label, role = label_for(feed, kind, msg)
        if role is None:
            return None
        sink = self.sink
        wall_ms = wall_ns / 1e6
        data = msg.get("data")

        if role == "bin":
            E = msg.get("E")
            if not excluded and E is not None and market:
                sink.add("delay", (market, label), wall_ms - E)
            return market, label

        ts = msg.get("ts")
        mk = market
        if mk is None:
            if isinstance(data, dict):
                mk = data.get("m")
            elif isinstance(data, list) and data:
                mk = data[0].get("m")
        tracked = mk in self.merged

        # v2 `ts` is the SEND time: recv - ts is pure transport (+ venue clock offset)
        if kind == V2 and ts is not None and mk and not excluded and not backfill:
            sink.add("transport", (mk, label), wall_ms - ts)

        if role == "trades":
            trades = data if isinstance(data, list) else []
            T = max((t.get("T") or 0) for t in trades) if trades else 0
            if not excluded and not backfill and T:
                sink.add("delay", (mk, label), wall_ms - T)
                if ts is not None:
                    sink.add("pub_lag", (mk, label), ts - T)
            if tracked and feed == "ext_trades" and not backfill:
                mb = self.merged[mk]
                race = self.race[mk]
                for t in trades:
                    try:
                        changed = mb.on_trade(t.get("S"), float(t["p"]), float(t["q"]), int(t["T"]))
                    except (KeyError, TypeError, ValueError):
                        continue
                    if changed and not excluded:
                        race.observe("trade_prov", mb.state(), wall_ns)
            self._sample_age(mk, wall_ns, excluded)
            return mk, label

        if not excluded and ts is not None and mk:
            sink.add("delay", (mk, label), wall_ms - ts)
            if market is None:
                sink.add("delay", ("ALL", label), wall_ms - ts)

        if role == "d1" and isinstance(data, dict):
            if tracked:
                touch = touch_from_levels(data)
                if feed in D1_FOR_MERGE and ts is not None:
                    self.merged[mk].on_d1(feed, ts, touch)
                if not excluded:
                    self.race[mk].observe(label, touch, wall_ns, ts)
                self._sample_age(mk, wall_ns, excluded)
            return mk, label

        if role == "full" and isinstance(data, dict) and market is not None:
            t = (msg.get("type") or data.get("t") or "").upper()
            book = self._book(label, mk)
            replay = book.apply(t.endswith("SNAPSHOT"), data, ts)
            if replay is not None:
                sink.count("replay_exact" if replay else "replay_mismatch", (mk, label))
            if tracked and book.valid:
                if label == FULL_FOR_MERGE:
                    self.merged[mk].on_full(ts)
                if not excluded:
                    self.race[mk].observe(label, book.top(), wall_ns, ts)
                    if self.compute_hold and label in HOLD_PAIR and ts is not None:
                        self._hold(mk, label, book, ts, wall_ns)
                self._sample_age(mk, wall_ns, excluded)
            return mk, label

        return mk, label

    def _sample_age(self, mk: str, wall_ns: int, excluded: bool) -> None:
        nxt = self.age_next.get(mk, 0)
        if wall_ns < nxt:
            return
        self.age_next[mk] = wall_ns + AGE_SAMPLE_NS
        mb = self.merged.get(mk)
        if excluded or mb is None or mb.src_ts is None:
            return
        self.sink.add("touch_age", (mk, "merged"), wall_ns / 1e6 - mb.src_ts)

    def _hold(self, mk: str, label: str, book: OrderBook, ts: int, wall_ns: int) -> None:
        mine = self.hold.setdefault((mk, label), OrderedDict())
        other_label = HOLD_PAIR[1] if label == HOLD_PAIR[0] else HOLD_PAIR[0]
        other = self.hold.setdefault((mk, other_label), OrderedDict())
        for d in (mine, other):
            while d and wall_ns - next(iter(d.values()))[1] > HOLD_WINDOW_NS:
                d.popitem(last=False)
        h = book.state_key(HOLD_LEVELS)
        hit = other.pop(h, None)
        if hit is not None:
            if label == HOLD_PAIR[0]:
                v1_ts, v1_wall, v2_ts, v2_wall = ts, wall_ns, hit[0], hit[1]
            else:
                v1_ts, v1_wall, v2_ts, v2_wall = hit[0], hit[1], ts, wall_ns
            self.sink.add("hold_ts", (mk, "full"), v2_ts - v1_ts)
            self.sink.add("hold_arrival", (mk, "full"), (v1_wall - v2_wall) / 1e6)
        elif h not in mine:
            mine[h] = (ts, wall_ns)
