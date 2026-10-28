"""One coroutine per socket: connect, subscribe, stamp, parse, reconnect with backoff (SPEC §4.1, §4.5)."""

from __future__ import annotations

import asyncio
import logging
import random
import time

import orjson
import websockets
from websockets.asyncio.client import connect

from .feeds import BIN, V1_TRADES, V2, FeedSpec, header

log = logging.getLogger("conn")

BACKOFF_MIN, BACKOFF_MAX = 0.5, 30.0
RPC_PING_S = 15.0


class SeqGap(Exception):
    pass


class Conn:
    def __init__(self, spec: FeedSpec, tracker) -> None:
        from .book import CatchupDetector

        self._Catchup = CatchupDetector
        self.spec = spec
        self.t = tracker
        self.conn_id = 0
        self.ws = None
        self.peer = None
        self.connected_mono = 0
        self.last_msg_mono = 0
        self.last_seq = None
        self.catchup = CatchupDetector()
        self.trade_topics_seen: set = set()
        self.first_trades = True
        self.total_msgs = 0
        self.minute_msgs: dict = {}
        self.minute_bytes = 0
        self.gaps = 0
        self.connects = 0
        self.silent_events = 0
        self._rpc_id = 1000
        self._rpc_pings: dict[int, int] = {}

    # ------------------------------------------------------------------ lifecycle
    async def run(self) -> None:
        spec, t = self.spec, self.t
        backoff = BACKOFF_MIN
        while not t.stop.is_set():
            self.conn_id += 1
            started = time.monotonic()
            reason = "closed"
            try:
                async with connect(
                    spec.url,
                    user_agent_header=t.cfg["user_agent"],
                    max_size=None,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=2,
                ) as ws:
                    self._on_connect(ws)
                    for sub in spec.subscribe:
                        await ws.send(orjson.dumps(sub).decode())
                    pinger = asyncio.create_task(self._rpc_pinger(ws)) if spec.kind == V2 else None
                    try:
                        await self._recv_loop(ws)
                    finally:
                        if pinger:
                            pinger.cancel()
            except SeqGap:
                reason = "seq_gap"
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.ConnectionClosed as e:
                reason = f"closed: {e}"
            except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
                reason = f"{type(e).__name__}: {e}"
            except Exception as e:  # never let one socket kill the process
                log.exception("%s: unexpected error", spec.key)
                reason = f"{type(e).__name__}: {e}"
            finally:
                if self.ws is not None:
                    t.writer.meta({"type": "disconnect", "feed": spec.key, "conn_id": self.conn_id, "reason": reason})
                self.ws = None
            if t.stop.is_set():
                break
            if time.monotonic() - started > 60:
                backoff = BACKOFF_MIN
            delay = backoff * random.uniform(0.5, 1.5)
            log.warning("%s conn %d ended (%s); reconnect in %.1fs", spec.key, self.conn_id, reason, delay)
            try:
                await asyncio.wait_for(t.stop.wait(), delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, BACKOFF_MAX)

    def _on_connect(self, ws) -> None:
        self.ws = ws
        self.connected_mono = time.perf_counter_ns()
        self.last_msg_mono = 0
        self.last_seq = None
        self.catchup = self._Catchup()
        self.trade_topics_seen = set()
        self.first_trades = True
        self.connects += 1
        try:
            self.peer = ws.remote_address[0] if ws.remote_address else None
        except Exception:
            self.peer = None
        self.t.writer.meta({
            "type": "connect", "feed": self.spec.key, "name": self.spec.name, "market": self.spec.market,
            "conn_id": self.conn_id, "peer": self.peer, "url": self.spec.url, "mono_ns": self.connected_mono,
        })
        log.info("%s conn %d connected to %s", self.spec.key, self.conn_id, self.peer)

    async def _rpc_pinger(self, ws) -> None:
        while True:
            await asyncio.sleep(RPC_PING_S)
            self._rpc_id += 1
            self._rpc_pings[self._rpc_id] = time.perf_counter_ns()
            await ws.send(orjson.dumps({"jsonrpc": "2.0", "id": self._rpc_id, "method": "ping"}).decode())

    async def ws_ping(self) -> float | None:
        """WebSocket ping->pong RTT in ms on the live socket."""
        ws = self.ws
        if ws is None:
            return None
        try:
            t0 = time.perf_counter_ns()
            waiter = await ws.ping()
            await asyncio.wait_for(waiter, 5)
            return (time.perf_counter_ns() - t0) / 1e6
        except Exception:
            return None

    # ------------------------------------------------------------------ receive path
    async def _recv_loop(self, ws) -> None:
        recv = ws.recv
        handle = self._handle
        while True:
            raw = await recv(decode=False)
            # stamp first, before anything else
            wall = time.time_ns()
            mono = time.perf_counter_ns()
            if handle(raw, wall, mono):
                raise SeqGap()

    def _handle(self, raw, wall: int, mono: int) -> bool:
        spec, t = self.spec, self.t
        if isinstance(raw, str):
            raw = raw.encode()
        try:
            msg = orjson.loads(raw)
            stored = raw
        except orjson.JSONDecodeError:
            msg = None
            stored = raw.decode("utf-8", "replace")
        self.last_msg_mono = mono
        self.total_msgs += 1
        self.minute_bytes += len(raw)
        kind = spec.kind

        if kind == V2 and isinstance(msg, dict) and "seq" not in msg:
            self._rpc_reply(msg)
        ts, seq, T, topic, m = header(kind, msg)

        gap = False
        if seq is not None and spec.seq_check:
            if self.last_seq is not None and seq != self.last_seq + 1:
                gap = True
            self.last_seq = seq

        is_trade = kind == V1_TRADES or (kind == V2 and isinstance(msg, dict) and str(msg.get("type", "")).upper().startswith("TRADE"))
        backfill = False
        catchup = False
        if is_trade:
            if kind == V1_TRADES:
                backfill = self.first_trades
                self.first_trades = False
            else:
                backfill = topic not in self.trade_topics_seen
                self.trade_topics_seen.add(topic)
        elif ts is not None and kind != BIN:
            catchup = self.catchup.update(ts, mono)
        warm = (mono - self.connected_mono) < t.warmup_ns
        excluded = catchup or warm or backfill

        res = t.engine.process(spec.name, kind, spec.market, self.conn_id, wall, msg, excluded, backfill)
        proc_us = (time.perf_counter_ns() - mono) // 1000
        mk = res[0] if res else (spec.market or m)
        if res:
            self.minute_msgs[res] = self.minute_msgs.get(res, 0) + 1
            t.sink.add("proc", res, proc_us / 1000)
        t.writer.put((spec.name, mk, self.conn_id, topic, wall, mono, ts, T, seq, gap, catchup, backfill, warm,
                      proc_us, stored))
        if gap:
            self.gaps += 1
            t.writer.meta({"type": "seq_gap", "feed": spec.key, "conn_id": self.conn_id, "got": seq})
            log.warning("%s SEQ GAP on conn %d at seq %s", spec.key, self.conn_id, seq)
            return spec.reconnect_on_gap
        return False

    def _rpc_reply(self, msg: dict) -> None:
        rid = msg.get("id")
        try:
            rid_i = int(rid)
        except (TypeError, ValueError):
            rid_i = None
        if rid_i in self._rpc_pings:
            rtt = (time.perf_counter_ns() - self._rpc_pings.pop(rid_i)) / 1e6
            self.t.writer.meta({"type": "rpc_ping", "feed": self.spec.key, "conn_id": self.conn_id, "rtt_ms": rtt})
            return
        if "error" in msg:
            log.warning("%s RPC error: %s", self.spec.key, msg)
        self.t.writer.meta({"type": "rpc_reply", "feed": self.spec.key, "conn_id": self.conn_id, "msg": msg})
