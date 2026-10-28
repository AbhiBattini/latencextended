"""Order books, connect-time replay detection, the merged quoter view (SPEC §3.5) and the touch race (§5.5).

Everything here is pure and synchronous so the live tracker and the offline report run exactly the same code.
"""

from __future__ import annotations

from collections import OrderedDict, deque

from .stats import Histogram

# (bid px, bid qty, ask px, ask qty); a side may be None when empty
Touch = tuple


def _levels(levels, key: str) -> dict[float, float]:
    out = {}
    for lv in levels or ():
        q = float(lv[key])
        if q > 0:
            out[float(lv["p"])] = q
    return out


class OrderBook:
    """Full-depth book from v1/v2 `orderbooks` snapshots + deltas. Delta `c` (resulting qty) is SET, never added."""

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.valid = False
        self.last_ts: int | None = None
        self.best_bid: float | None = None
        self.best_ask: float | None = None
        self.snapshots = 0
        self.missing_c = 0

    def reset(self) -> None:
        self.__init__()

    def apply(self, is_snapshot: bool, data: dict, ts: int | None):
        """Apply one message. Returns None, or for a mid-stream snapshot (True|False) = exact replay match."""
        replay = None
        if is_snapshot:
            bids = _levels(data.get("b"), "q")
            asks = _levels(data.get("a"), "q")
            if self.valid:
                replay = bids == self.bids and asks == self.asks
            self.bids, self.asks = bids, asks
            self.best_bid = max(bids) if bids else None
            self.best_ask = min(asks) if asks else None
            self.valid = True
            self.snapshots += 1
        else:
            if not self.valid:
                return None  # deltas before the first snapshot cannot be applied
            self._delta(self.bids, data.get("b"), True)
            self._delta(self.asks, data.get("a"), False)
        if ts is not None:
            self.last_ts = ts
        return replay

    def _delta(self, side: dict, levels, is_bid: bool) -> None:
        if not levels:
            return
        recompute = False
        best = self.best_bid if is_bid else self.best_ask
        for lv in levels:
            p = float(lv["p"])
            c = lv.get("c")
            if c is None:
                self.missing_c += 1
                c = lv["q"]
            c = float(c)
            if c <= 0:
                if side.pop(p, None) is not None and p == best:
                    recompute = True
            else:
                side[p] = c
                if best is None or (p > best if is_bid else p < best):
                    best = p
        if recompute:
            best = (max(side) if is_bid else min(side)) if side else None
        if is_bid:
            self.best_bid = best
        else:
            self.best_ask = best

    def top(self) -> Touch | None:
        if not self.valid:
            return None
        bb, ba = self.best_bid, self.best_ask
        return (bb, self.bids[bb] if bb is not None else None, ba, self.asks[ba] if ba is not None else None)

    def state_key(self, n: int = 25) -> int:
        """Hash of the top `n` levels per side (used to match the same book state across feeds)."""
        import heapq

        b = heapq.nlargest(n, self.bids)
        a = heapq.nsmallest(n, self.asks)
        return hash((tuple((p, self.bids[p]) for p in b), tuple((p, self.asks[p]) for p in a)))


def touch_from_levels(data: dict) -> Touch:
    b = data.get("b") or ()
    a = data.get("a") or ()
    return (
        float(b[0]["p"]) if b else None,
        float(b[0]["q"]) if b else None,
        float(a[0]["p"]) if a else None,
        float(a[0]["q"]) if a else None,
    )


class CatchupDetector:
    """Connect-time replay detector (SPEC §2.7).

    excess = (venue_ts - venue_ts_first) - (mono - mono_first) rises while the venue replays its buffer faster than
    real time. Rows are catch-up until the running max of `excess` has not grown by more than `flat_ms` for `hold_s`,
    then the flag is sticky-false for the connection. Downward moves (network jitter on live data) never reset it.
    """

    __slots__ = ("flat_ms", "hold_ns", "max_ns", "ts0", "mono0", "ref", "band_start", "live")

    def __init__(self, flat_ms: float = 5.0, hold_s: float = 1.5, max_s: float = 60.0) -> None:
        self.flat_ms = flat_ms
        self.hold_ns = int(hold_s * 1e9)
        self.max_ns = int(max_s * 1e9)
        self.ts0 = None
        self.mono0 = 0
        self.ref = 0.0
        self.band_start = 0
        self.live = False

    def update(self, venue_ts_ms, mono_ns: int) -> bool:
        if self.live:
            return False
        if venue_ts_ms is None:
            return True
        if self.ts0 is None:
            self.ts0 = venue_ts_ms
            self.mono0 = mono_ns
            self.band_start = mono_ns
            return True
        excess = (venue_ts_ms - self.ts0) - (mono_ns - self.mono0) / 1e6
        if excess > self.ref + self.flat_ms:
            self.ref = excess
            self.band_start = mono_ns
        if mono_ns - self.band_start >= self.hold_ns or mono_ns - self.mono0 >= self.max_ns:
            self.live = True
            return False
        return True


class MergedBook:
    """The view a quoter reads (SPEC §3.5): full depth from `ext_full`, touch overridden by `depth=1`, crossed stale
    full-book levels hidden, provisional updates from trades. Every touch carries its source and venue timestamp."""

    def __init__(self, market: str, full: OrderBook) -> None:
        self.market = market
        self.full = full
        self.touch: list | None = None  # [bp, bq, ap, aq]
        self.src: str | None = None
        self.src_ts: int | None = None
        self.from_d1 = False
        self.d1_ts = -1
        self._seen: OrderedDict = OrderedDict()

    def state(self) -> Touch | None:
        return tuple(self.touch) if self.touch else None

    def on_d1(self, src: str, ts: int, touch: Touch) -> bool:
        """A depth=1 message (either socket; first arrival wins). Returns True if the merged touch changed."""
        key = (ts, touch)
        if key in self._seen:
            return False
        self._seen[key] = None
        if len(self._seen) > 256:
            self._seen.popitem(last=False)
        if ts < self.d1_ts:
            return False  # older than a depth=1 state we already applied (the other socket was faster)
        if self.full.valid and self.full.last_ts is not None and ts < self.full.last_ts:
            return False  # full book is newer
        self.d1_ts = ts
        old = self.touch
        self.touch = list(touch)
        self.src, self.src_ts, self.from_d1 = src, ts, True
        return old != self.touch

    def on_full(self, ts: int | None) -> bool:
        """Called after the full book applied a message. Full wins only if strictly newer than the depth=1 touch."""
        if not self.full.valid:
            return False
        if self.from_d1 and ts is not None and ts <= self.d1_ts:
            return False
        top = self.full.top()
        old = self.touch
        self.touch = list(top)
        self.src, self.src_ts, self.from_d1 = "ext_full", ts, False
        return old != self.touch

    def on_trade(self, side: str, px: float, qty: float, match_ts: int) -> bool:
        """Provisional update: the aggressor eats the resting opposite side at/through its price. Only applied when
        the trade is strictly newer than the touch (a same-ms book update already shows the post-trade book)."""
        if not self.touch or self.src_ts is None or match_ts <= self.src_ts:
            return False
        bp, bq, ap, aq = self.touch
        if side == "BUY" and ap is not None and ap <= px:
            aq = round(aq - qty, 12)
            if aq <= 0:
                nxt = [p for p in self.full.asks if p > ap and p > px] if self.full.valid else []
                ap = min(nxt) if nxt else None
                aq = self.full.asks[ap] if ap is not None else None
            self.touch = [bp, bq, ap, aq]
        elif side == "SELL" and bp is not None and bp >= px:
            bq = round(bq - qty, 12)
            if bq <= 0:
                nxt = [p for p in self.full.bids if p < bp and p < px] if self.full.valid else []
                bp = max(nxt) if nxt else None
                bq = self.full.bids[bp] if bp is not None else None
            self.touch = [bp, bq, ap, aq]
        else:
            return False
        self.src, self.src_ts = "trade_prov", match_ts
        return True

    def view(self, n: int = 10) -> tuple[list, list]:
        """Top `n` levels per side of the merged view. Full-book levels crossing a newer depth=1 touch are hidden
        (the stored book is never mutated for this)."""
        bids = sorted(self.full.bids.items(), reverse=True) if self.full.valid else []
        asks = sorted(self.full.asks.items()) if self.full.valid else []
        if self.touch and self.src != "ext_full":
            bp, bq, ap, aq = self.touch
            if bp is not None:
                bids = [(bp, bq)] + [(p, q) for p, q in bids if p < bp]
            if ap is not None:
                asks = [(ap, aq)] + [(p, q) for p, q in asks if p > ap]
        return bids[:n], asks[:n]


class TouchRace:
    """For every distinct touch state, the first receipt time on every source that shows it (SPEC §5.5).

    A state is open for `window_s` after its first sighting, then finalised into statistics and forgotten, so memory
    stays bounded over a 24 h run.
    """

    def __init__(self, market: str, window_s: float = 5.0, pair=("ext_d1_a", "ext_d1_b")) -> None:
        self.market = market
        self.window_ns = int(window_s * 1e9)
        self.pair = pair
        self.open: dict = {}
        self.order: deque = deque()
        self.states = 0
        self.shown: dict[str, int] = {}
        self.earliest: dict[str, int] = {}
        self.lag: dict[str, Histogram] = {}
        self.pair_gain = Histogram()  # a - min(a, b), ms
        self.pair_delay_a = Histogram()  # recv_a - ts
        self.pair_delay_ab = Histogram()  # min(recv_a, recv_b) - ts
        self.minute_lags: dict[str, list] = {}  # live per-minute (drained by the summary)

    def observe(self, src: str, touch: Touch | None, recv_ns: int, ts: int | None = None) -> None:
        if touch is None or None in touch:
            return
        e = self.open.get(touch)
        if e is None:
            self.open[touch] = {src: (recv_ns, ts)}
            self.order.append((recv_ns, touch))
        elif src not in e:
            e[src] = (recv_ns, ts)
        self.expire(recv_ns)

    def expire(self, now_ns: int | None = None) -> None:
        o = self.order
        while o and (now_ns is None or now_ns - o[0][0] > self.window_ns):
            _, touch = o.popleft()
            e = self.open.pop(touch, None)
            if e:
                self._finalise(e)

    def _finalise(self, e: dict) -> None:
        self.states += 1
        first = min(v[0] for v in e.values())
        for src, (t, _) in e.items():
            lag = (t - first) / 1e6
            self.shown[src] = self.shown.get(src, 0) + 1
            if t == first:
                self.earliest[src] = self.earliest.get(src, 0) + 1
            h = self.lag.get(src)
            if h is None:
                h = self.lag[src] = Histogram()
            h.add(lag)
            self.minute_lags.setdefault(src, []).append(lag)
        a = e.get(self.pair[0])
        if a is not None:
            b = e.get(self.pair[1])
            best = min(a[0], b[0]) if b else a[0]
            self.pair_gain.add((a[0] - best) / 1e6)
            if a[1] is not None:
                self.pair_delay_a.add(a[0] / 1e6 - a[1])
                self.pair_delay_ab.add(best / 1e6 - a[1])

    def summary(self) -> dict:
        srcs = {}
        for src in sorted(self.shown):
            srcs[src] = {
                "shown": self.shown[src],
                "share": self.shown[src] / self.states if self.states else None,
                "earliest_share": self.earliest.get(src, 0) / self.states if self.states else None,
                "lag": self.lag[src].summary(),
            }
        return {
            "states": self.states,
            "sources": srcs,
            "pair_gain": self.pair_gain.summary(),
            "pair_delay_a": self.pair_delay_a.summary(),
            "pair_delay_ab": self.pair_delay_ab.summary(),
        }
