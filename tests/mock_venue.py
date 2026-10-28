"""A local stand-in for Extended (v1 streams, v2 JSON-RPC, REST /info/markets) and Binance bookTicker.

Used by the end-to-end tests and handy for developing without network access:

    python -m tests.mock_venue --port 8765
    # then point config endpoints at ws://127.0.0.1:8765/v1, ws://127.0.0.1:8765/v2/rpc,
    # ws://127.0.0.1:8765/binance/ws and http://127.0.0.1:8765/api/v1

Behaviour mirrors SPEC §2: one random-walk book per market; depth=1 pushes on change (≤10 ms); the full book sends a
SNAPSHOT then 100 ms DELTAs (with `c` = resulting qty) and re-snapshots every `resnap_s`; trades send a backfill batch
first; v1 book streams replay ~2 s of old-timestamped messages on connect; v2 `seq` is per connection starting at 0
and `ts` is the send time. Any path containing "NOPE" connects and then stays silent (muted feed).
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from http import HTTPStatus

import orjson
from websockets.asyncio.server import serve

TICK = 0.001


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class SimMarket:
    def __init__(self, name: str, mid: float, rng: random.Random) -> None:
        self.name = name
        self.rng = rng
        m = int(round(mid / TICK))
        self.bids = {m - 1 - i: round(rng.uniform(1, 50), 2) for i in range(60)}
        self.asks = {m + i: round(rng.uniform(1, 50), 2) for i in range(60)}
        self.version = 0
        self.change_ts = now_ms()
        self.trades: list[dict] = []
        self.trade_id = 1000
        # a few old trades for the connect backfill
        for i in range(3):
            self._trade("BUY", min(self.asks), 0.1, now_ms() - 30_000 + i)

    def _trade(self, side, px_t, q, ts) -> None:
        self.trade_id += 1
        self.trades.append({"i": self.trade_id, "m": self.name, "S": side, "tT": "TRADE", "T": ts,
                            "p": f"{px_t * TICK:.3f}", "q": f"{q:.2f}"})

    def step(self) -> None:
        r = self.rng.random()
        ts = now_ms()
        bb, ba = max(self.bids), min(self.asks)
        if r < 0.35:  # requote at the touch
            side = self.bids if self.rng.random() < 0.5 else self.asks
            p = bb if side is self.bids else ba
            side[p] = round(self.rng.uniform(1, 50), 2)
        elif r < 0.55:  # deep change
            side = self.bids if self.rng.random() < 0.5 else self.asks
            p = (bb - self.rng.randint(1, 50)) if side is self.bids else (ba + self.rng.randint(1, 50))
            if self.rng.random() < 0.3 and p in side:
                del side[p]
            else:
                side[p] = round(self.rng.uniform(1, 50), 2)
        elif r < 0.70:  # improve inside the spread
            if ba - bb > 1:
                if self.rng.random() < 0.5:
                    self.bids[bb + 1] = round(self.rng.uniform(1, 20), 2)
                else:
                    self.asks[ba - 1] = round(self.rng.uniform(1, 20), 2)
            else:
                self.bids.pop(bb, None) if len(self.bids) > 5 else None
        elif r < 0.85:  # trade eats part/all of the touch
            if self.rng.random() < 0.5:
                q = min(self.asks[ba], round(self.rng.uniform(0.1, 30), 2))
                self._trade("BUY", ba, q, ts)
                left = round(self.asks[ba] - q, 2)
                if left <= 0 and len(self.asks) > 5:
                    del self.asks[ba]
                else:
                    self.asks[ba] = max(left, 0.01)
            else:
                q = min(self.bids[bb], round(self.rng.uniform(0.1, 30), 2))
                self._trade("SELL", bb, q, ts)
                left = round(self.bids[bb] - q, 2)
                if left <= 0 and len(self.bids) > 5:
                    del self.bids[bb]
                else:
                    self.bids[bb] = max(left, 0.01)
        else:
            return
        self.version += 1
        self.change_ts = ts

    def top(self) -> tuple:
        bb, ba = max(self.bids), min(self.asks)
        return bb, self.bids[bb], ba, self.asks[ba]

    def levels(self, side: dict, rev: bool) -> list[dict]:
        return [{"q": f"{q:.2f}", "p": f"{p * TICK:.3f}"} for p, q in sorted(side.items(), reverse=rev)]

    def copy(self) -> tuple[dict, dict]:
        return dict(self.bids), dict(self.asks)


def d1_payload(mk: SimMarket) -> dict:
    bb, bq, ba, aq = mk.top()
    return {"t": "SNAPSHOT", "m": mk.name, "b": [{"q": f"{bq:.2f}", "p": f"{bb * TICK:.3f}"}],
            "a": [{"q": f"{aq:.2f}", "p": f"{ba * TICK:.3f}"}], "d": "1"}


def diff(old: dict, new: dict) -> list[dict]:
    out = []
    for p in old.keys() | new.keys():
        o, n = old.get(p, 0.0), new.get(p, 0.0)
        if o != n:
            out.append({"q": f"{n - o:.2f}", "p": f"{p * TICK:.3f}", "c": f"{n:.2f}" if n else "0"})
    return out


class Venue:
    def __init__(self, markets=("HYPE-USD", "BTC-USD", "ETH-USD", "SOL-USD"), seed: int = 7, resnap_s: float = 10.0,
                 replay: bool = True) -> None:
        rng = random.Random(seed)
        mids = {"HYPE-USD": 40.0, "BTC-USD": 95.0, "ETH-USD": 33.0, "SOL-USD": 15.0}
        self.markets = {m: SimMarket(m, mids.get(m, 10.0), rng) for m in markets}
        self.resnap_s = resnap_s
        self.replay = replay
        self.connections = 0

    async def sim(self) -> None:
        while True:
            for mk in self.markets.values():
                mk.step()
            await asyncio.sleep(0.008)

    # ------------------------------------------------------------------ HTTP (REST)
    def process_request(self, connection, request):
        if request.headers.get("Upgrade", "").lower() == "websocket":
            if not request.headers.get("User-Agent"):
                return connection.respond(HTTPStatus.FORBIDDEN, "User-Agent required\n")
            return None
        if request.path.startswith("/api/v1/info/markets"):
            rows = [{"name": m, "status": "ACTIVE", "active": True, "isRfq": False} for m in self.markets]
            rows.append({"name": "FOO-USD", "status": "ACTIVE", "active": True, "isRfq": True})
            resp = connection.respond(HTTPStatus.OK, orjson.dumps({"status": "OK", "data": rows}).decode())
            resp.headers["Content-Type"] = "application/json"
            return resp
        return connection.respond(HTTPStatus.NOT_FOUND, "not found\n")

    # ------------------------------------------------------------------ WebSocket routing
    async def handler(self, ws) -> None:
        self.connections += 1
        path = ws.request.path
        try:
            if "NOPE" in path:
                await ws.wait_closed()
            elif path.startswith("/v2/rpc"):
                await self.v2(ws)
            elif path.startswith("/binance/ws/"):
                await self.binance(ws, path.rsplit("/", 1)[1].split("@")[0].upper())
            elif path.startswith("/v1/orderbooks"):
                rest = path[len("/v1/orderbooks"):]
                d1 = "depth=1" in rest
                mname = rest.split("?")[0].strip("/")
                if mname.startswith("rfq/"):
                    mname = mname[4:]
                if not mname:
                    await self.v1_all(ws, d1)
                elif mname in self.markets:
                    await (self.v1_d1(ws, self.markets[mname]) if d1 else self.v1_full(ws, self.markets[mname]))
                else:
                    await ws.wait_closed()
            elif path.startswith("/v1/publicTrades/"):
                await self.v1_trades(ws, self.markets[path.rsplit("/", 1)[1]])
            elif path.startswith("/v1/prices/"):
                await self.v1_price(ws, self.markets[path.rsplit("/", 1)[1]], "MP" if "/mark/" in path else "IP")
            else:
                await ws.close(1008, "unknown stream")
        except Exception:
            pass

    async def v1_d1(self, ws, mk: SimMarket) -> None:
        seq = 0
        if self.replay:  # connect-time replay with original (old) timestamps, sent fast
            base = now_ms() - 2000
            for i in range(20):
                seq += 1
                await ws.send(orjson.dumps({"type": "SNAPSHOT", "data": d1_payload(mk), "ts": base + i * 100, "seq": seq}))
                await asyncio.sleep(0.002)
        last = None
        while True:
            top = mk.top()
            if top != last:
                last = top
                seq += 1
                await ws.send(orjson.dumps({"type": "SNAPSHOT", "data": d1_payload(mk), "ts": mk.change_ts, "seq": seq}))
            await asyncio.sleep(0.005)

    async def v1_full(self, ws, mk: SimMarket) -> None:
        seq = 1
        b, a = mk.copy()
        await ws.send(orjson.dumps({"type": "SNAPSHOT", "data": {"t": "SNAPSHOT", "m": mk.name,
                                    "b": mk.levels(b, True), "a": mk.levels(a, False), "d": "f"},
                                    "ts": mk.change_ts, "seq": seq}))
        last_snap = time.monotonic()
        while True:
            await asyncio.sleep(0.1)
            seq += 1
            nb, na = mk.copy()
            ts = mk.change_ts - 2  # event time: slightly before the depth=1 push of the same change
            msg = {"type": "DELTA", "data": {"t": "DELTA", "m": mk.name, "b": diff(b, nb), "a": diff(a, na), "d": "f"},
                   "ts": ts, "seq": seq}
            b, a = nb, na
            await ws.send(orjson.dumps(msg))
            if time.monotonic() - last_snap >= self.resnap_s:  # in-stream re-snapshot of the same state
                last_snap = time.monotonic()
                seq += 1
                await ws.send(orjson.dumps({"type": "SNAPSHOT", "data": {"t": "SNAPSHOT", "m": mk.name,
                                            "b": mk.levels(b, True), "a": mk.levels(a, False), "d": "f"},
                                            "ts": ts, "seq": seq}))

    async def v1_all(self, ws, d1: bool) -> None:
        seq = 0
        lasts = {}
        while True:
            for mk in self.markets.values():
                top = mk.top()
                if top != lasts.get(mk.name):
                    lasts[mk.name] = top
                    seq += 1
                    data = d1_payload(mk) if d1 else {"t": "DELTA", "m": mk.name, "b": [], "a": [], "d": "f"}
                    await ws.send(orjson.dumps({"type": "SNAPSHOT" if d1 else "DELTA", "data": data,
                                                "ts": mk.change_ts, "seq": seq}))
            await asyncio.sleep(0.01 if d1 else 0.1)

    async def v1_trades(self, ws, mk: SimMarket) -> None:
        seq = 1
        idx = len(mk.trades)
        await ws.send(orjson.dumps({"data": mk.trades[max(0, idx - 3):idx], "ts": now_ms(), "seq": seq}))
        while True:
            await asyncio.sleep(0.005)
            if len(mk.trades) > idx:
                new = mk.trades[idx:]
                idx = len(mk.trades)
                seq += 1
                await ws.send(orjson.dumps({"data": new, "ts": now_ms(), "seq": seq}))

    async def v1_price(self, ws, mk: SimMarket, typ: str) -> None:
        seq = 0
        while True:
            seq += 1
            bb, _, ba, _ = mk.top()
            await ws.send(orjson.dumps({"type": typ, "data": {"m": mk.name, "p": f"{(bb + ba) / 2 * TICK:.4f}",
                                                              "ts": now_ms()}, "ts": now_ms(), "seq": seq}))
            await asyncio.sleep(1.5)

    async def binance(self, ws, sym: str) -> None:
        u = 1
        mk = next((m for m in self.markets.values() if m.name.split("-")[0] == sym.replace("USDT", "")), None)
        while True:
            u += 3
            bb, bq, ba, aq = mk.top() if mk else (1, 1, 2, 1)
            t = now_ms()
            await ws.send(orjson.dumps({"e": "bookTicker", "u": u, "s": sym, "b": f"{bb * TICK:.3f}", "B": f"{bq:.2f}",
                                        "a": f"{ba * TICK:.3f}", "A": f"{aq:.2f}", "T": t, "E": t}))
            await asyncio.sleep(0.01)

    async def v2(self, ws) -> None:
        state = {"seq": -1}
        lock = asyncio.Lock()
        tasks = []

        async def send(sub: str, typ: str, data) -> None:
            async with lock:
                state["seq"] += 1
                await ws.send(orjson.dumps({"type": typ, "subscription": sub, "data": data, "ts": now_ms(),
                                            "seq": state["seq"]}))

        async def d1_topic(mk):
            last = None
            while True:
                top = mk.top()
                if top != last:
                    last = top
                    await send(f"orderbooks.1.{mk.name}", "ORDERBOOKS.SNAPSHOT", d1_payload(mk))
                await asyncio.sleep(0.005)

        async def full_topic(mk):
            b, a = mk.copy()
            await send(f"orderbooks.{mk.name}", "ORDERBOOKS.SNAPSHOT",
                       {"t": "SNAPSHOT", "m": mk.name, "b": mk.levels(b, True), "a": mk.levels(a, False), "d": "f"})
            while True:
                await asyncio.sleep(0.1)
                nb, na = mk.copy()
                await send(f"orderbooks.{mk.name}", "ORDERBOOKS.DELTA",
                           {"t": "DELTA", "m": mk.name, "b": diff(b, nb), "a": diff(a, na), "d": "f"})
                b, a = nb, na

        async def trades_topic(mk):
            idx = len(mk.trades)
            await send(f"trades.{mk.name}", "TRADES", mk.trades[max(0, idx - 3):idx])
            while True:
                await asyncio.sleep(0.005)
                if len(mk.trades) > idx:
                    new = mk.trades[idx:]
                    idx = len(mk.trades)
                    await send(f"trades.{mk.name}", "TRADES", new)

        try:
            async for raw in ws:
                req = orjson.loads(raw)
                rid, method = req.get("id"), req.get("method")
                if method == "ping":
                    await ws.send(orjson.dumps({"id": str(rid), "result": "pong", "jsonrpc": "2.0"}))
                    continue
                if method != "subscribe":
                    await ws.send(orjson.dumps({"id": str(rid), "error": {"code": -32601, "message": "unknown"}}))
                    continue
                p = req["params"]
                mname = p["selector"].get("market")
                mk = self.markets.get(mname)
                if mk is None:
                    await ws.send(orjson.dumps({"id": str(rid), "error": {"code": 1, "message": "unknown market"}}))
                    continue
                if p["scope"] == "orderbooks":
                    depth = str(p["selector"].get("depth"))
                    sub = f"orderbooks.1.{mname}" if depth == "1" else f"orderbooks.{mname}"
                    tasks.append(asyncio.create_task(d1_topic(mk) if depth == "1" else full_topic(mk)))
                else:
                    sub = f"trades.{mname}"
                    tasks.append(asyncio.create_task(trades_topic(mk)))
                await ws.send(orjson.dumps({"id": str(rid), "result": {"method": "subscribe", "status": "OK",
                                                                       "subscription": sub}, "jsonrpc": "2.0"}))
        finally:
            for t in tasks:
                t.cancel()


async def run(port: int, ready: asyncio.Event | None = None, **kw) -> None:
    v = Venue(**kw)
    sim = asyncio.create_task(v.sim())
    async with serve(v.handler, "127.0.0.1", port, process_request=v.process_request, max_size=None):
        if ready:
            ready.set()
        await asyncio.Future()
    sim.cancel()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--resnap-s", type=float, default=10.0)
    a = ap.parse_args()
    asyncio.run(run(a.port, resnap_s=a.resnap_s))


if __name__ == "__main__":
    main()
