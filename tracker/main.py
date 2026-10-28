"""CLI entry point.

    python -m tracker.main --config config.yaml --minutes 60      # long run
    python -m tracker.main --config config.yaml --test            # 15-minute test mode (SPEC §4.6)

Exit codes: 0 ok; 2 test-mode continuity checks failed; 3 clock not converged with --require-clock.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import platform
import signal
import socket
import sys
import time
import urllib.request
from pathlib import Path

import orjson

from . import config as config_mod
from . import __version__
from .clock import ClockMonitor
from .conn import Conn
from .engine import Engine
from .feeds import PRODUCTION, build_feeds, rfq_feed
from .live import LiveSink, build_minute
from .netprobe import hosts_from_cfg, probe
from .stats import pcts
from .writer import Writer

log = logging.getLogger("tracker")


def _imds() -> dict:
    """EC2 instance metadata (IMDSv2). Returns {} off EC2. Proxies are bypassed (link-local)."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base = "http://169.254.169.254/latest"
    try:
        req = urllib.request.Request(f"{base}/api/token", method="PUT",
                                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"})
        token = opener.open(req, timeout=0.5).read().decode()
    except Exception:
        return {}
    out = {}
    for key, path in (("instance_id", "meta-data/instance-id"), ("instance_type", "meta-data/instance-type"),
                      ("az", "meta-data/placement/availability-zone"),
                      ("az_id", "meta-data/placement/availability-zone-id"),
                      ("region", "meta-data/placement/region")):
        try:
            r = urllib.request.Request(f"{base}/{path}", headers={"X-aws-ec2-metadata-token": token})
            out[key] = opener.open(r, timeout=0.5).read().decode()
        except Exception:
            out[key] = None
    return out


def host_info() -> dict:
    return {
        "hostname": socket.gethostname(),
        "kernel": platform.release(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "tracker_version": __version__,
        "cpu_count": os.cpu_count(),
        **_imds(),
    }


def http_get_json(url: str, ua: str, timeout: float = 10.0):
    req = urllib.request.Request(url, headers={"User-Agent": ua, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return orjson.loads(r.read())


class Tracker:
    def __init__(self, cfg: dict, writer: Writer, specs, test_mode: bool) -> None:
        self.cfg = cfg
        self.writer = writer
        self.test_mode = test_mode
        self.stop = asyncio.Event()
        self.warmup_ns = int(cfg["warmup_s"] * 1e9)
        self.sink = LiveSink()
        markets = [m["market"] for m in cfg["markets"]]
        self.engine = Engine(markets, self.sink, cfg["race_window_s"], compute_hold=False)
        self.conns: list[Conn] = [Conn(s, self) for s in specs]
        self.clock = ClockMonitor(cfg)
        self.loop_lags: list[float] = []
        self.last_rtt: dict = {}
        self.tasks: list[asyncio.Task] = []
        self.silent_total = 0

    def start_conn(self, spec) -> None:
        c = Conn(spec, self)
        self.conns.append(c)
        self.tasks.append(asyncio.create_task(c.run(), name=spec.key))

    async def run(self, duration_s: float) -> None:
        for c in self.conns:
            self.tasks.append(asyncio.create_task(c.run(), name=c.spec.key))
        for coro in (self._watchdog(), self._loop_lag(), self._minutes(), self._netprobe(), self._clock(),
                     self._markets()):
            self.tasks.append(asyncio.create_task(coro))
        try:
            await asyncio.wait_for(self.stop.wait(), duration_s)
        except asyncio.TimeoutError:
            pass
        self.stop.set()
        log.info("stopping")
        for c in self.conns:
            if c.ws is not None:
                try:
                    await asyncio.wait_for(c.ws.close(), 3)
                except Exception:
                    pass
        for tk in self.tasks:
            tk.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self._emit_minute(time.time_ns())
        self.engine.finish()

    # ------------------------------------------------------------ background tasks
    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            now = time.perf_counter_ns()
            for c in self.conns:
                if c.ws is None:
                    continue
                since = max(c.last_msg_mono, c.connected_mono)
                if now - since > c.spec.silent_s * 1e9:
                    c.silent_events += 1
                    self.silent_total += 1
                    c.last_msg_mono = now
                    log.error("SILENT FEED %s: no message for %.0fs on conn %d - reconnecting",
                              c.spec.key, (now - since) / 1e9, c.conn_id)
                    self.writer.meta({"type": "silent", "feed": c.spec.key, "conn_id": c.conn_id,
                                      "silent_s": (now - since) / 1e9, "is_book": c.spec.is_book})
                    asyncio.create_task(c.ws.close())

    async def _loop_lag(self) -> None:
        while True:
            t0 = time.perf_counter_ns()
            await asyncio.sleep(0.01)
            self.loop_lags.append((time.perf_counter_ns() - t0) / 1e6 - 10.0)

    async def _minutes(self) -> None:
        while True:
            now = time.time()
            await asyncio.sleep(60 - now % 60)
            await self._emit_minute(time.time_ns())

    async def _emit_minute(self, wall_ns: int) -> None:
        values, counts = self.sink.swap()
        lags, self.loop_lags = self.loop_lags, []
        race_lags = {}
        for mk, r in self.engine.race.items():
            race_lags[mk], r.minute_lags = r.minute_lags, {}
        conns = []
        for c in self.conns:
            msgs, c.minute_msgs = c.minute_msgs, {}
            nbytes, c.minute_bytes = c.minute_bytes, 0
            conns.append({"key": c.spec.key, "conn_id": c.conn_id, "connected": c.ws is not None,
                          "msgs": {f"{k[0]}|{k[1]}": v for k, v in msgs.items()}, "bytes": nbytes,
                          "gaps": c.gaps, "reconnects": max(c.connects - 1, 0), "silent": c.silent_events,
                          "peer": c.peer})
        minute = await asyncio.to_thread(build_minute, wall_ns - 60_000_000_000, values, counts, conns, race_lags,
                                         lags, self.clock.last, self.last_rtt, self.writer.drops)
        self.writer.meta({"type": "looplag", **minute["loop_lag"]})
        await asyncio.to_thread(self._append_minute, minute)
        total = sum(sum(c["msgs"].values()) for c in conns)
        d1 = minute["metrics"].get("delay", {})
        hint = " ".join(f"{k.split('|')[1]}={v['p50']:.1f}" for k, v in list(d1.items())[:6] if v.get("p50") is not None)
        b = (self.clock.last or {}).get("bound_ms")
        log.info("minute: %d msgs, loop-lag p99 %s ms, clock bound %s ms, drops %d | delay p50 %s", total,
                 f"{minute['loop_lag']['p99']:.2f}" if minute["loop_lag"]["p99"] is not None else "-",
                 f"{b:.3f}" if isinstance(b, (int, float)) else "-", self.writer.drops, hint)

    def _append_minute(self, minute: dict) -> None:
        with open(self.writer.run_dir / "minutes.jsonl", "ab") as f:
            f.write(orjson.dumps(minute, option=orjson.OPT_NON_STR_KEYS) + b"\n")

    async def _netprobe(self) -> None:
        interval = 30 if self.test_mode else self.cfg["netprobe_interval_s"]
        hosts = hosts_from_cfg(self.cfg)
        await asyncio.sleep(5)
        while True:
            results = await asyncio.to_thread(probe, hosts)
            for r in results:
                self.writer.meta(r)
                self.last_rtt[r["ip"]] = {"host": r["host"], "min_ms": r["min_ms"], "median_ms": r["median_ms"]}
            pings = await asyncio.gather(*(c.ws_ping() for c in self.conns))
            for c, rtt in zip(self.conns, pings):
                if rtt is not None:
                    self.writer.meta({"type": "wsping", "feed": c.spec.key, "conn_id": c.conn_id, "peer": c.peer,
                                      "rtt_ms": rtt})
            await asyncio.sleep(interval)

    async def _clock(self) -> None:
        while True:
            s = await asyncio.to_thread(self.clock.sample)
            self.writer.meta({"type": "clock", **s})
            await asyncio.sleep(self.cfg["clock"]["sample_s"])

    async def _markets(self) -> None:
        url = self.cfg["endpoints"]["rest"].rstrip("/") + "/info/markets"
        tracked = {m["market"] for m in self.cfg["markets"]}
        rfq_started: set = set()
        while True:
            try:
                body = await asyncio.to_thread(http_get_json, url, self.cfg["user_agent"])
                rows = body.get("data", body) if isinstance(body, dict) else body
                info = []
                for row in rows or []:
                    if isinstance(row, dict) and row.get("name") in tracked:
                        info.append({k: row.get(k) for k in ("name", "status", "active", "isRfq")})
                n_rfq = sum(1 for row in rows or [] if isinstance(row, dict) and row.get("isRfq"))
                self.writer.meta({"type": "markets", "tracked": info, "n_markets": len(rows or []), "n_rfq": n_rfq})
                for row in info:
                    log.info("market %s status=%s active=%s isRfq=%s", row["name"], row["status"], row["active"],
                             row["isRfq"])
                    if row.get("isRfq") and row["name"] not in rfq_started:
                        log.warning("!!! %s IS AN RFQ MARKET: the normal book is indicative only; post-only orders "
                                    "are rejected; also subscribing orderbooks/rfq/%s", row["name"], row["name"])
                        rfq_started.add(row["name"])
                        self.start_conn(rfq_feed(self.cfg, row["name"]))
                missing = tracked - {r["name"] for r in info}
                if missing:
                    log.warning("tracked markets not listed by /info/markets: %s", sorted(missing))
            except Exception as e:
                log.warning("markets poll failed: %s", e)
                self.writer.meta({"type": "markets_error", "error": str(e)})
            await asyncio.sleep(self.cfg["markets_poll_s"])


def _setup_logging(run_dir: Path) -> None:
    fmt = logging.Formatter("%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s", "%Y-%m-%dT%H:%M:%S")
    fmt.converter = time.gmtime
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    fh = logging.FileHandler(run_dir / "tracker.log")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def capture(cfg: dict, args, run_dir: Path, mode: str, minutes: float) -> dict:
    """Clock check + capture. Returns run info used by the checks."""
    clock = ClockMonitor(cfg)
    wait_s = cfg["clock"]["wait_s"] if (args.test or args.require_clock) else 0
    first = await clock.wait_converged(wait_s)
    clock_ok = clock.ok(first)
    if not clock_ok:
        b = first.get("bound_ms")
        log.error("CLOCK NOT CONVERGED: source=%s bound=%s ms (max %.3f)", first.get("source"), b,
                  clock.max_error_ms)
        if args.require_clock and not args.test:
            return {"abort": 3, "clock_first": first}
    else:
        log.info("clock ok: source=%s bound=%s ms", first.get("source"), first.get("bound_ms"))

    only = set(args.only_feeds.split(",")) if args.only_feeds else None
    controls = args.test or args.controls or cfg.get("controls")
    specs = build_feeds(cfg, controls=bool(controls), only=only)
    writer = Writer(run_dir, maxsize=cfg["writer_queue"])
    info = host_info()
    writer.meta({"type": "run_start", "mode": mode, "minutes": minutes, "run_id": run_dir.name, "host": info,
                 "markets": cfg["markets"], "feeds": [s.key for s in specs],
                 "production": [s.key for s in specs if s.name in PRODUCTION], "controls": bool(controls),
                 "clock_first": first, "clock_ok_at_start": clock_ok, "warmup_s": cfg["warmup_s"],
                 "clock_max_error_ms": clock.max_error_ms, "argv": sys.argv})
    log.info("run %s (%s, %.1f min) on %s %s %s: %d sockets", run_dir.name, mode, minutes,
             info.get("instance_type") or info["hostname"], info.get("az_id") or "", info["kernel"], len(specs))
    tracker = Tracker(cfg, writer, specs, test_mode=args.test)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, tracker.stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    t0 = time.time_ns()
    await tracker.run(minutes * 60)
    writer.meta({"type": "run_stop", "duration_s": (time.time_ns() - t0) / 1e9, "writer_drops": writer.drops,
                 "written": writer.written, "silent_total": tracker.silent_total})
    await asyncio.to_thread(writer.close)
    log.info("capture finished: %d records, %d dropped", writer.written, writer.drops)
    return {"clock_first": first, "clock_ok": clock_ok}


def _run_loop(coro):
    try:
        import uvloop

        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as r:
            return r.run(coro)
    except ImportError:
        return asyncio.run(coro)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="tracker.main", description="Extended feed latency tracker")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--minutes", type=float, help="capture duration (long run)")
    ap.add_argument("--test", action="store_true", help="15-minute test mode, then report + exit code (SPEC §4.6)")
    ap.add_argument("--test-minutes", type=float, help="override the test-mode capture length")
    ap.add_argument("--run-id", help="run folder name (default: <mode>-<UTC time>)")
    ap.add_argument("--data-dir", help="override output_dir")
    ap.add_argument("--markets", help="comma-separated Extended markets (Binance symbol derived as ASSETusdt)")
    ap.add_argument("--only-feeds", help="comma-separated feed names to run (e.g. ext_d1_a,ext_rpc_d1)")
    ap.add_argument("--controls", action="store_true", help="also run the optional control feeds")
    ap.add_argument("--require-clock", action="store_true", help="refuse to start if the clock bound exceeds the max")
    ap.add_argument("--s3-bucket", help="override s3_bucket")
    ap.add_argument("--no-report", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args(argv)

    cfg = config_mod.load(args.config)
    if args.markets:
        cfg["markets"] = [{"market": m, "binance": m.split("-")[0].lower() + "usdt"} for m in args.markets.split(",")]
    if args.data_dir:
        cfg["output_dir"] = args.data_dir
    if args.s3_bucket:
        cfg["s3_bucket"] = args.s3_bucket
    mode = "test" if args.test else "run"
    if args.test:
        minutes = args.test_minutes if args.test_minutes is not None else cfg["test_minutes"]
    else:
        minutes = args.minutes if args.minutes is not None else cfg["run_minutes"]
    run_id = args.run_id or f"{mode}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    run_dir = Path(cfg["output_dir"]) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _setup_logging(run_dir)

    t_start = time.monotonic()
    res = _run_loop(capture(cfg, args, run_dir, mode, minutes))
    if res.get("abort"):
        return res["abort"]

    code = 0
    if not args.no_report:
        from analysis.report import build_report, console_summary

        t_rep = time.monotonic()
        summary = build_report(run_dir, out_dir=run_dir)
        txt = console_summary(summary)
        print(txt, flush=True)
        (run_dir / "summary.txt").write_text(txt)
        log.info("report built in %.1fs (total %.1f min)", time.monotonic() - t_rep, (time.monotonic() - t_start) / 60)
        failed = [c for c in summary["checks"] if not c["ok"]]
        if args.test and failed:
            code = 2
    bucket = cfg.get("s3_bucket")
    if bucket and not args.no_upload:
        from .upload import upload_run

        upload_run(run_dir, bucket, run_id)
    return code


if __name__ == "__main__":
    sys.exit(main())
