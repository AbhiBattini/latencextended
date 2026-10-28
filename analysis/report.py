"""Raw archive -> report.md + summary.csv + summary.json (SPEC §5).

    python -m analysis.report --data data/<run-id> [--out report.md]

The report is reproducible from the run folder alone: every message is replayed through the same `tracker.engine`
code the live tracker uses (books, merged view, touch race), with the exclusions of SPEC §5: catch-up / backfill
rows, the first `warmup_s` after every connect, and minutes whose clock-error bound exceeds the configured maximum.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

import orjson

from tracker.engine import Engine
from tracker.feeds import ALL_MARKET_FEEDS, FEED_KINDS, PRODUCTION
from tracker.stats import QNAMES, Histogram
from tracker.writer import iter_raw, read_meta

# US baseline (SPEC §3.1, HYPE-USD): label -> (description, p01, p50, p90)
US_BASELINE = {
    "ext_d1_a": ("v1 depth=1", 90, 100, 123),
    "ext_trades": ("v1 publicTrades (vs match T)", 93, 102, 131),
    "ext_full": ("v1 full book", 106, 141, 196),
    "ext_rpc.d1": ("v2 depth=1 (multiplexed)", 89, 93, 116),
    "ext_rpc_d1": ("v2 depth=1 (single topic)", 89, 93, 116),
    "ext_rpc.full": ("v2 full", 90, 95, 112),
    "ctl_all_d1": ("all-markets depth=1", 90, 101, 134),
    "ctl_all_full": ("all-markets full", 105, 135, 207),
    "ctl_mark": ("mark price", 87, 92, 110),
    "ctl_index": ("index price", 87, 92, 110),
    "bin_top": ("Binance bookTicker", 87, 93, 116),
}
US_ONE_WAY_MS = 89
FEED_ORDER = ["ext_d1_a", "ext_d1_b", "ext_rpc.d1", "ext_rpc_d1", "ext_full", "ext_rpc.full", "ext_trades",
              "ext_rpc.trades", "ext_rfq", "bin_top", "ctl_all_d1", "ctl_all_full", "ctl_mark", "ctl_index",
              "trade_prov"]


def _order(label: str) -> tuple:
    return (FEED_ORDER.index(label) if label in FEED_ORDER else 99, label)


class ReportSink:
    def __init__(self) -> None:
        self.h: dict = defaultdict(lambda: defaultdict(Histogram))
        self.c: dict = defaultdict(lambda: defaultdict(int))

    def add(self, metric, key, value) -> None:
        self.h[metric][key].add(value)

    def count(self, metric, key, n=1) -> None:
        self.c[metric][key] += n

    def get(self, metric, key) -> dict | None:
        h = self.h.get(metric, {}).get(key)
        return h.summary() if h is not None and h.n else None


def _f(v, nd=1) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.{nd}f}"
    return str(v)


def md_table(headers, rows) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(_f(x) if not isinstance(x, str) else x for x in r) + " |")
    return "\n".join(out)


def _dist_row(d: dict | None, nd=1) -> list:
    if not d:
        return ["-"] * 7
    return [_f(d.get(q), nd) for q in QNAMES] + [_f(d.get("max"), nd), str(d.get("n", 0))]


# --------------------------------------------------------------------------------------------------------- analysis
class ClockTimeline:
    """Step function of clock samples: bound / correction at any wall time."""

    def __init__(self, samples: list[dict], max_ms: float) -> None:
        self.s = sorted((x for x in samples if x.get("sample_wall_ns")), key=lambda x: x["sample_wall_ns"])
        self.t = [x["sample_wall_ns"] for x in self.s]
        self.max_ms = max_ms
        self.exclusion = True
        self.unchecked = not self.s or all(x.get("source") == "none" for x in self.s)

    def at(self, wall_ns: int) -> dict | None:
        if not self.s:
            return None
        i = bisect.bisect_right(self.t, wall_ns) - 1
        return self.s[max(i, 0)]

    def bad(self, sample: dict | None) -> bool:
        if not self.exclusion or sample is None or sample.get("source") == "none":
            return False
        b = sample.get("bound_ms")
        return b is None or b > self.max_ms


def analyse(run_dir: Path) -> dict:
    run_dir = Path(run_dir)
    meta = read_meta(run_dir)
    by_type = defaultdict(list)
    for m in meta:
        by_type[m.get("type")].append(m)
    start = (by_type.get("run_start") or [{}])[0]
    stop = (by_type.get("run_stop") or [{}])[-1]
    markets = [m["market"] for m in start.get("markets", [])] or ["HYPE-USD"]
    warmup_s = start.get("warmup_s", 15)
    max_ms = start.get("clock_max_error_ms", 1.0)
    warnings: list[str] = []

    clock_samples = list(by_type.get("clock", []))
    if start.get("clock_first"):
        cf = dict(start["clock_first"])
        cf.setdefault("sample_wall_ns", start.get("wall_ns"))
        clock_samples.insert(0, cf)
    clock = ClockTimeline(clock_samples, max_ms)
    if clock.s and not clock.unchecked and all(clock.bad(x) for x in clock.s):
        clock.exclusion = False
        warnings.append(f"CLOCK: every clock sample exceeds the {max_ms} ms error bound - minute exclusion disabled "
                        "(otherwise nothing would remain). Absolute delays are only as good as the clock.")

    sink = ReportSink()
    engine = Engine(markets, sink, race_window_s=5.0, compute_hold=True)
    msgs = defaultdict(int)
    nbytes = defaultdict(int)
    first_last: dict = {}
    hourly = defaultdict(lambda: [0, 0])
    gaps = defaultdict(int)
    gaps_hourly = defaultdict(int)
    catchup_rows = defaultdict(int)
    backfill_rows = defaultdict(int)
    feed_data_msgs = defaultdict(int)
    excluded_clock_rows = 0
    total = 0
    seg = None
    t0 = time.monotonic()
    minute_cache: dict = {}

    for seg_start, line in iter_raw(run_dir):
        if seg != seg_start:
            if seg is not None:
                engine.reset()
            seg = seg_start
        try:
            rec = orjson.loads(line)
        except orjson.JSONDecodeError:
            continue
        total += 1
        feed = rec["feed"]
        kind = FEED_KINDS.get(feed)
        if kind is None:
            continue
        wall = rec["recv_wall_ns"]
        minute = wall // 60_000_000_000
        cs = minute_cache.get(minute)
        if cs is None:
            smp = clock.at(wall)
            corr = (smp or {}).get("correction_ms") or 0.0
            cs = minute_cache[minute] = (clock.bad(smp), int(corr * 1e6))
        clock_bad, corr_ns = cs
        msg = rec.get("raw")
        if isinstance(msg, str):
            try:
                msg = orjson.loads(msg)
            except orjson.JSONDecodeError:
                msg = None
        excluded = bool(rec.get("is_catchup") or rec.get("is_backfill") or rec.get("warm") or clock_bad)
        if clock_bad and not (rec.get("is_catchup") or rec.get("is_backfill") or rec.get("warm")):
            excluded_clock_rows += 1
        market_param = None if feed in ALL_MARKET_FEEDS else rec.get("mkt")
        res = engine.process(feed, kind, market_param, rec.get("conn_id"), wall + corr_ns, msg, excluded,
                             bool(rec.get("is_backfill")))
        fkey = f"{feed}:{market_param or 'ALL'}"
        if rec.get("seq_gap"):
            gaps[fkey] += 1
            gaps_hourly[(wall // 3_600_000_000_000, feed)] += 1
        if rec.get("is_catchup"):
            catchup_rows[fkey] += 1
        if rec.get("is_backfill"):
            backfill_rows[fkey] += 1
        if res is None:
            continue
        feed_data_msgs[fkey] += 1
        mk, label = res
        k = (mk, label)
        msgs[k] += 1
        nbytes[k] += len(line)
        fl = first_last.get(k)
        if fl is None:
            first_last[k] = [wall, wall]
        else:
            fl[1] = wall
        hk = (wall // 3_600_000_000_000, label)
        hourly[hk][0] += 1
        hourly[hk][1] += len(line)
        if market_param is None:
            msgs[("ALL", label)] += 1
            nbytes[("ALL", label)] += len(line)
        pu = rec.get("proc_us")
        if pu is not None:
            sink.add("proc", k, pu / 1000)
            sink.add("proc", ("*", "*"), pu / 1000)
    engine.finish()
    analysis_s = time.monotonic() - t0

    duration_s = stop.get("duration_s")
    if not duration_s and first_last:
        duration_s = (max(v[1] for v in first_last.values()) - min(v[0] for v in first_last.values())) / 1e9
    duration_s = duration_s or 1.0

    # ---------------------------------------------------------------- network / clock / continuity from meta
    tcp = defaultdict(list)
    tcp_host = {}
    for r in by_type.get("netprobe", []):
        tcp[r["ip"]].extend(r.get("tcp_ms") or [])
        tcp_host[r["ip"]] = r["host"]
    tcp_rows = []
    for ip, xs in sorted(tcp.items(), key=lambda kv: (tcp_host[kv[0]], kv[0])):
        tcp_rows.append({"host": tcp_host[ip], "ip": ip, "n": len(xs), "min_ms": min(xs) if xs else None,
                         "median_ms": statistics.median(xs) if xs else None})
    wsping = defaultdict(list)
    peers = defaultdict(set)
    for r in by_type.get("wsping", []):
        wsping[r["feed"]].append(r["rtt_ms"])
    rpcping = defaultdict(list)
    for r in by_type.get("rpc_ping", []):
        rpcping[r["feed"]].append(r["rtt_ms"])
    connects = defaultdict(int)
    connects_hourly = defaultdict(int)
    for r in by_type.get("connect", []):
        connects[r["feed"]] += 1
        if r.get("peer"):
            peers[r["feed"]].add(r["peer"])
        connects_hourly[(r["wall_ns"] // 3_600_000_000_000, r["feed"].split(":")[0])] += 1
    silent = by_type.get("silent", [])
    silent_hourly = defaultdict(int)
    for r in silent:
        silent_hourly[(r["wall_ns"] // 3_600_000_000_000, r["feed"].split(":")[0])] += 1

    def host_min(pred) -> float | None:
        xs = [r["min_ms"] for r in tcp_rows if r["min_ms"] is not None and pred(r["host"])]
        return min(xs) if xs else None

    ext_host = lambda h: "extended" in h or h.startswith("127.") or h == "localhost"  # noqa: E731
    bin_host = lambda h: "binance" in h  # noqa: E731
    ext_rtt, bin_rtt = host_min(ext_host), host_min(bin_host)
    if ext_rtt is None and tcp_rows:
        ext_rtt = min((r["min_ms"] for r in tcp_rows if r["min_ms"] is not None), default=None)

    in_run = [x for x in clock.s if x.get("source") not in (None,)]
    bounds = [x["bound_ms"] for x in in_run if isinstance(x.get("bound_ms"), (int, float))]
    clock_info = {
        "sources": sorted({x.get("source") for x in in_run}),
        "refids": sorted({str(x.get("refid") or x.get("ref_name")) for x in in_run if x.get("refid") or x.get("ref_name")}),
        "phc": sorted({d for x in in_run for d in (x.get("phc") or [])}),
        "samples": len(in_run),
        "bound_p50_ms": statistics.median(bounds) if bounds else None,
        "bound_max_ms": max(bounds) if bounds else None,
        "share_within_max": (sum(1 for b in bounds if b <= max_ms) / len(bounds)) if bounds else None,
        "max_error_ms": max_ms,
        "unchecked": clock.unchecked,
        "exclusion_applied": clock.exclusion,
        "excluded_rows": excluded_clock_rows,
        "ok_at_start": start.get("clock_ok_at_start"),
        "first": start.get("clock_first"),
    }
    if start.get("clock_ok_at_start") is False:
        warnings.insert(0, "CLOCK NOT CONVERGED at start (bound %s ms > %s ms)" % (
            _f((start.get("clock_first") or {}).get("bound_ms"), 3), max_ms))
    looplag = by_type.get("looplag", [])
    ll_p99 = [x["p99"] for x in looplag if x.get("p99") is not None]
    loop_info = {"minutes": len(ll_p99), "p99_median_ms": statistics.median(ll_p99) if ll_p99 else None,
                 "p99_max_ms": max(ll_p99) if ll_p99 else None,
                 "p50_median_ms": statistics.median([x["p50"] for x in looplag if x.get("p50") is not None])
                 if looplag else None}
    if loop_info["p99_median_ms"] is not None and loop_info["p99_median_ms"] > 3:
        warnings.append(f"EVENT LOOP LAG p99 {loop_info['p99_median_ms']:.1f} ms (median minute) - receive "
                        "timestamps are contaminated by Python scheduling.")
    drops = stop.get("writer_drops", 0)
    if drops:
        warnings.append(f"WRITER DROPPED {drops} records (queue full) - the archive is incomplete.")
    for r in by_type.get("markets", []):
        for row in r.get("tracked", []):
            if row.get("isRfq"):
                w = f"RFQ MARKET {row['name']}: its normal order book is indicative only."
                if w not in warnings:
                    warnings.append(w)

    # ---------------------------------------------------------------- per-market tables
    per_market = {}
    all_mk = sorted({k[0] for k in msgs}, key=lambda m: (m not in markets, markets.index(m) if m in markets else 0, m))
    for mk in all_mk:
        labels = sorted({k[1] for k in msgs if k[0] == mk}, key=_order)
        feeds = {}
        for label in labels:
            k = (mk, label)
            feeds[label] = {
                "msgs": msgs[k],
                "msgs_per_s": msgs[k] / duration_s,
                "bytes_per_s": nbytes[k] / duration_s,
                "delay": sink.get("delay", k),
                "transport": sink.get("transport", k),
                "pub_lag": sink.get("pub_lag", k),
                "proc": sink.get("proc", k),
                "replay_exact": sink.c["replay_exact"].get(k, 0),
                "replay_mismatch": sink.c["replay_mismatch"].get(k, 0),
            }
        entry = {"feeds": feeds}
        if mk in engine.race:
            entry["race"] = engine.race[mk].summary()
            entry["touch_age"] = sink.get("touch_age", (mk, "merged"))
            entry["hold_ts"] = sink.get("hold_ts", (mk, "full"))
            entry["hold_arrival"] = sink.get("hold_arrival", (mk, "full"))
        per_market[mk] = entry

    # venue clock offset estimates (SPEC §5.4)
    offsets = {}
    for mk in markets:
        f = per_market.get(mk, {}).get("feeds", {})
        tp = [f[l]["transport"]["p01"] for l in ("ext_rpc_d1", "ext_rpc.d1", "ext_rpc.full")
              if f.get(l, {}).get("transport")]
        bd = (f.get("bin_top", {}).get("delay") or {}).get("p01")
        offsets[mk] = {
            "extended_ms": (min(tp) - ext_rtt / 2) if tp and ext_rtt is not None else None,
            "extended_p01_transport_ms": min(tp) if tp else None,
            "binance_ms": (bd - bin_rtt / 2) if bd is not None and bin_rtt is not None else None,
            "binance_p01_delay_ms": bd,
        }
    for mk, e in per_market.items():
        for label, f in e["feeds"].items():
            d = f.get("delay")
            if d and d.get("p01") is not None and d["p01"] < 0 and mk != "ALL":
                warnings.append(f"NEGATIVE p01 delay on {mk} {label} ({d['p01']:.2f} ms): the venue clock is ahead "
                                "of ours or ours is wrong - see venue clock offsets.")

    # ---------------------------------------------------------------- hourly throughput
    hours = sorted({h for h, _ in hourly})
    hourly_rows = []
    for h in hours:
        for label in sorted({l for hh, l in hourly if hh == h}, key=_order):
            n, b = hourly[(h, label)]
            feed = label.split(".")[0]
            hourly_rows.append({
                "hour": time.strftime("%Y-%m-%dT%H:00Z", time.gmtime(h * 3600)), "label": label, "msgs": n,
                "msgs_per_s": n / 3600 if len(hours) > 1 else n / duration_s,
                "kb_per_s": b / 1024 / (3600 if len(hours) > 1 else duration_s),
                "gaps": gaps_hourly.get((h, label), 0) if "." not in label else gaps_hourly.get((h, feed), 0),
                "connects": connects_hourly.get((h, feed), 0), "silent": silent_hourly.get((h, feed), 0),
            })

    # ---------------------------------------------------------------- checks (SPEC §4.6 / §5.10)
    checks = []
    production = start.get("production") or []
    c_ok = clock.unchecked or (bool(start.get("clock_ok_at_start")) and (clock_info["share_within_max"] or 0) >= 0.99)
    if clock.unchecked and "none" in clock_info["sources"]:
        detail = "clock check disabled (clock.mode = none)"
    elif clock.unchecked:
        c_ok = False
        detail = "no clock source available"
    else:
        detail = (f"bound p50 {_f(clock_info['bound_p50_ms'], 3)} ms, max {_f(clock_info['bound_max_ms'], 3)} ms, "
                  f"{_f((clock_info['share_within_max'] or 0) * 100)} % of samples <= {max_ms} ms")
    checks.append({"name": "clock_bound", "ok": bool(c_ok), "detail": detail})
    dead = []
    for fk in production:
        name, mk = fk.split(":", 1)
        if name == "ext_rpc":
            for lab in ("ext_rpc.d1", "ext_rpc.full"):
                if msgs.get((mk, lab), 0) == 0:
                    dead.append(f"{fk} ({lab.split('.')[1]})")
        elif feed_data_msgs.get(fk, 0) == 0:
            dead.append(fk)
    checks.append({"name": "all_feeds_delivered", "ok": not dead,
                   "detail": "every production feed delivered data" if not dead else "no data: " + ", ".join(dead)})
    n_gaps = sum(gaps.values())
    checks.append({"name": "no_seq_gaps", "ok": n_gaps == 0,
                   "detail": f"{n_gaps} gaps" + (": " + ", ".join(f"{k}={v}" for k, v in gaps.items()) if gaps else "")})
    silent_book = [r for r in silent if r.get("is_book", True) and r["feed"] in production]
    checks.append({"name": "no_silent_book_feed", "ok": not silent_book,
                   "detail": f"{len(silent_book)} silent-feed events" + (
                       ": " + ", ".join(sorted({r['feed'] for r in silent_book})) if silent_book else "")})
    exact = sum(v for (mk, l), v in sink.c["replay_exact"].items() if l == "ext_full")
    mism = sum(v for (mk, l), v in sink.c["replay_mismatch"].items() if l == "ext_full")
    if any(fk.startswith("ext_full:") for fk in production):
        checks.append({"name": "full_book_replay_exact", "ok": exact >= 1 and mism == 0,
                       "detail": f"{exact} exact / {mism} mismatched mid-stream snapshots (ext_full)"})

    return {
        "run": {
            "run_id": start.get("run_id", run_dir.name), "mode": start.get("mode"), "minutes": start.get("minutes"),
            "duration_s": duration_s, "host": start.get("host", {}), "markets": markets,
            "feeds": start.get("feeds", []), "controls": start.get("controls"), "warmup_s": warmup_s,
            "records": total, "analysis_s": analysis_s,
            "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start.get("wall_ns", 0) / 1e9)),
        },
        "warnings": warnings,
        "markets": per_market,
        "venue_offset": offsets,
        "clock": clock_info,
        "network": {"tcp": tcp_rows, "ext_min_rtt_ms": ext_rtt, "binance_min_rtt_ms": bin_rtt,
                    "wsping": {k: {"n": len(v), "min_ms": min(v), "median_ms": statistics.median(v)}
                               for k, v in sorted(wsping.items())},
                    "rpc_ping": {k: {"n": len(v), "min_ms": min(v), "median_ms": statistics.median(v)}
                                 for k, v in sorted(rpcping.items())},
                    "peers": {k: sorted(v) for k, v in sorted(peers.items())}},
        "processing": {"proc": sink.get("proc", ("*", "*")), "loop_lag": loop_info},
        "continuity": {"gaps": dict(gaps), "connects": dict(connects),
                       "reconnects": {k: v - 1 for k, v in connects.items() if v > 1},
                       "silent": [{"feed": r["feed"], "conn_id": r.get("conn_id"), "silent_s": r.get("silent_s")}
                                  for r in silent],
                       "writer_drops": drops, "catchup_rows": dict(catchup_rows), "backfill_rows": dict(backfill_rows)},
        "hourly": hourly_rows,
        "checks": checks,
    }


# --------------------------------------------------------------------------------------------------------- output
def _ref_market(s: dict) -> str | None:
    mks = [m for m in s["run"]["markets"] if m in s["markets"]]
    return "HYPE-USD" if "HYPE-USD" in mks else (mks[0] if mks else None)


def render_md(s: dict) -> str:
    r = s["run"]
    h = r["host"]
    out = [f"# Extended feed latency report — `{r['run_id']}`", ""]
    if r.get("mode") == "test":
        out += [f"**mode: test** ({_f(r['duration_s'] / 60)} min capture). *15-minute sample: p99 and max are "
                "indicative only; tail estimates need the 24 h run.*", ""]
    for w in s["warnings"]:
        out.append(f"> **WARNING** {w}")
    if s["warnings"]:
        out.append("")
    out += ["## Run metadata", "", md_table(["field", "value"], [
        ["start (UTC)", r["start_utc"]], ["duration", f"{_f(r['duration_s'] / 60)} min"],
        ["mode", str(r.get("mode"))],
        ["instance type", str(h.get("instance_type") or "-")], ["AZ / AZ ID", f"{h.get('az') or '-'} / {h.get('az_id') or '-'}"],
        ["host", str(h.get("hostname"))], ["kernel", str(h.get("kernel"))], ["python", str(h.get("python"))],
        ["clock", f"{', '.join(s['clock']['sources'])} ref {', '.join(s['clock']['refids']) or '-'}; PHC: "
                  f"{', '.join(s['clock']['phc']) or 'none'}"],
        ["markets", ", ".join(r["markets"])], ["sockets", str(len(r["feeds"]))],
        ["records", str(r["records"])], ["warm-up exclusion", f"{r['warmup_s']} s after each connect"],
    ]), ""]

    out += ["## Checks", "", md_table(["check", "result", "detail"],
                                       [[c["name"], "PASS" if c["ok"] else "**FAIL**", c["detail"]] for c in s["checks"]]), ""]

    out += ["## 5.1 Delivery delay (recv − venue ts; trades: recv − match T), ms", "",
            "v1 book `ts` is the event time (includes the venue batching hold); v2 `ts` is the send time; Binance uses `E`.", ""]
    for mk, e in s["markets"].items():
        rows = []
        for label, f in e["feeds"].items():
            rows.append([label, _f(f["msgs_per_s"], 2)] + _dist_row(f["delay"]))
        out += [f"### {mk}", "", md_table(["feed", "msg/s"] + list(QNAMES) + ["max", "n"], rows), ""]
        pl = [[label] + _dist_row(f["pub_lag"]) for label, f in e["feeds"].items() if f.get("pub_lag")]
        if pl:
            out += ["Venue publish lag (trade `ts − T`, clock-free):", "",
                    md_table(["feed"] + list(QNAMES) + ["max", "n"], pl), ""]

    out += ["## 5.2 Transport delay (v2 only: recv − send-time ts), ms", ""]
    rows = []
    for mk, e in s["markets"].items():
        for label, f in e["feeds"].items():
            if f.get("transport"):
                rows.append([mk, label] + _dist_row(f["transport"], 2))
    out += [md_table(["market", "feed"] + list(QNAMES) + ["max", "n"], rows) if rows else "_no v2 data_", ""]

    out += ["## 5.3 Full-book batch hold", "",
            "Same top-25 book state on v1 full and v2 full: `hold = v2 ts − v1 ts` (server-side hold); "
            "`arrival = v1 recv − v2 recv`. US baseline ≈ 45 ms median.", ""]
    rows = []
    for mk, e in s["markets"].items():
        if "hold_ts" in e:
            rows.append([mk, "hold (ts)"] + _dist_row(e["hold_ts"]))
            rows.append([mk, "arrival diff"] + _dist_row(e["hold_arrival"]))
    out += [md_table(["market", "metric"] + list(QNAMES) + ["max", "n"], rows), ""]

    c = s["clock"]
    out += ["## 5.4 Clock quality and venue clock offset", "", md_table(["field", "value"], [
        ["sources", ", ".join(c["sources"]) or "-"], ["samples", str(c["samples"])],
        ["error bound p50 / max (ms)", f"{_f(c['bound_p50_ms'], 3)} / {_f(c['bound_max_ms'], 3)}"],
        [f"samples within {c['max_error_ms']} ms", f"{_f((c['share_within_max'] or 0) * 100)} %" if c['share_within_max'] is not None else "-"],
        ["minute exclusion applied", str(c["exclusion_applied"])], ["rows excluded for clock", str(c["excluded_rows"])],
    ]), "", "`venue_offset ≈ p01(transport or delay) − min_RTT/2` (positive = venue clock ahead of ours):", ""]
    out += [md_table(["market", "Extended offset", "p01 v2 transport", "Binance offset", "p01 Binance delay"],
                     [[mk, _f(o["extended_ms"], 2), _f(o["extended_p01_transport_ms"], 2), _f(o["binance_ms"], 2),
                       _f(o["binance_p01_delay_ms"], 2)] for mk, o in s["venue_offset"].items()]),
            f"\nmin TCP RTT: Extended {_f(s['network']['ext_min_rtt_ms'], 3)} ms, Binance "
            f"{_f(s['network']['binance_min_rtt_ms'], 3)} ms", ""]

    out += ["## 5.5 Touch race", "",
            "For every distinct touch state (bid px, bid qty, ask px, ask qty): first receipt per source. "
            "Lag is vs the earliest source, ms.", ""]
    for mk, e in s["markets"].items():
        if "race" not in e:
            continue
        rc = e["race"]
        rows = []
        for src in sorted(rc["sources"], key=_order):
            x = rc["sources"][src]
            l = x["lag"]
            rows.append([src, _f(x["share"] * 100 if x["share"] is not None else None), _f(x["earliest_share"] * 100 if x["earliest_share"] is not None else None),
                         _f(l["p50"], 2), _f(l["p90"], 2), _f(l["p99"], 2), str(x["shown"])])
        g, da, dab = rc["pair_gain"], rc["pair_delay_a"], rc["pair_delay_ab"]
        out += [f"### {mk} ({rc['states']} states)", "",
                md_table(["source", "% states shown", "% earliest", "lag p50", "lag p90", "lag p99", "n"], rows), "",
                f"Racing two depth=1 sockets: `a − min(a,b)` p50 {_f(g['p50'], 2)} / p90 {_f(g['p90'], 2)} / p99 "
                f"{_f(g['p99'], 2)} ms; delay `a` alone p50/p90/p99 {_f(da['p50'])}/{_f(da['p90'])}/{_f(da['p99'])} "
                f"vs `min(a,b)` {_f(dab['p50'])}/{_f(dab['p90'])}/{_f(dab['p99'])} ms.", ""]
        if e.get("touch_age"):
            ta = e["touch_age"]
            out += [f"Merged-touch age (sampled every 100 ms, now − source ts): p50 {_f(ta['p50'])} / p90 "
                    f"{_f(ta['p90'])} / p99 {_f(ta['p99'])} ms.", ""]

    n = s["network"]
    out += ["## 5.6 Network probes", "", "TCP connect (SYN→established ≈ 1 RTT), ms:", "",
            md_table(["host", "ip", "min", "median", "n"],
                     [[t["host"], t["ip"], _f(t["min_ms"], 3), _f(t["median_ms"], 3), str(t["n"])] for t in n["tcp"]]), ""]
    if n["wsping"]:
        out += ["WebSocket ping→pong RTT per socket, ms:", "",
                md_table(["socket", "peer", "min", "median", "n"],
                         [[k, ", ".join(n["peers"].get(k, [])), _f(v["min_ms"], 3), _f(v["median_ms"], 3), str(v["n"])]
                          for k, v in n["wsping"].items()]), ""]
    if n["rpc_ping"]:
        out += ["v2 JSON-RPC ping RTT, ms:", "", md_table(["socket", "min", "median", "n"],
                [[k, _f(v["min_ms"], 3), _f(v["median_ms"], 3), str(v["n"])] for k, v in n["rpc_ping"].items()]), ""]

    p = s["processing"]
    out += ["## 5.7 Internal processing", "",
            "`recv → merged book updated` per message (perf_counter) and event-loop lag (10 ms timer lateness), ms.", ""]
    rows = [["all messages"] + _dist_row(p["proc"], 3)]
    for mk, e in s["markets"].items():
        for label, f in e["feeds"].items():
            if f.get("proc") and mk != "ALL":
                rows.append([f"{mk} {label}"] + _dist_row(f["proc"], 3))
    out += [md_table(["scope"] + list(QNAMES) + ["max", "n"], rows), "",
            f"Event-loop lag: per-minute p99 median {_f(p['loop_lag']['p99_median_ms'], 3)} ms, worst minute "
            f"{_f(p['loop_lag']['p99_max_ms'], 3)} ms, p50 {_f(p['loop_lag']['p50_median_ms'], 3)} ms.", ""]

    co = s["continuity"]
    out += ["## 5.8 Throughput and continuity", "",
            md_table(["hour", "feed", "msgs", "msg/s", "KB/s", "seq gaps", "connects", "silent"],
                     [[x["hour"], x["label"], str(x["msgs"]), _f(x["msgs_per_s"], 2), _f(x["kb_per_s"], 1),
                       str(x["gaps"]), str(x["connects"]), str(x["silent"])] for x in s["hourly"]]), "",
            f"Totals: seq gaps {sum(co['gaps'].values())}, reconnects {sum(co['reconnects'].values())}, "
            f"silent-feed events {len(co['silent'])}, writer drops {co['writer_drops']}.", ""]
    silent_n = defaultdict(int)
    for x in co["silent"]:
        silent_n[x["feed"]] += 1
    keys = sorted(set(co["connects"]) | set(co["gaps"]) | set(co["catchup_rows"]) | set(silent_n),
                  key=lambda k: (_order(k.split(":")[0]), k))
    out += ["Per socket (catch-up = connect-replay rows detected and excluded; backfill = trade backfill rows):", "",
            md_table(["socket", "connects", "seq gaps", "silent", "catch-up rows", "backfill rows", "peer IPs"],
                     [[k, str(co["connects"].get(k, 0)), str(co["gaps"].get(k, 0)), str(silent_n.get(k, 0)),
                       str(co["catchup_rows"].get(k, 0)), str(co["backfill_rows"].get(k, 0)),
                       ", ".join(s["network"]["peers"].get(k, []))] for k in keys]), ""]
    rows = []
    for mk, e in s["markets"].items():
        for label, f in e["feeds"].items():
            if f["replay_exact"] or f["replay_mismatch"]:
                rows.append([mk, label, str(f["replay_exact"]), str(f["replay_mismatch"])])
    if rows:
        out += ["Full-book replay check at every mid-stream venue SNAPSHOT:", "",
                md_table(["market", "feed", "exact", "mismatch"], rows), ""]

    ref = _ref_market(s)
    if ref:
        rows = []
        for label, (desc, u01, u50, u90) in US_BASELINE.items():
            f = s["markets"][ref]["feeds"].get(label)
            d = (f or {}).get("delay") or {}
            if not d:
                continue
            rows.append([desc, label, f"{u01} / {u50} / {u90}", f"{u50 - US_ONE_WAY_MS}",
                         f"{_f(d.get('p01'))} / {_f(d.get('p50'))} / {_f(d.get('p90'))}",
                         _f(u50 - d["p50"]) if d.get("p50") is not None else "-"])
        out += [f"## This run vs the US baseline (SPEC §3.1, {ref})", "",
                md_table(["feed", "label", "US p01/p50/p90", "expected p50 here", "this run p01/p50/p90", "p50 saved"], rows), ""]
    out += [f"_Report generated in {_f(r['analysis_s'])} s from {r['records']} archived records._", ""]
    return "\n".join(out)


def csv_rows(s: dict):
    yield ["section", "market", "feed", "metric", "n"] + list(QNAMES) + ["max", "extra"]

    def row(section, mk, label, metric, d, extra=""):
        if d:
            return [section, mk, label, metric, d.get("n")] + [d.get(q) for q in QNAMES] + [d.get("max"), extra]
        return None

    for mk, e in s["markets"].items():
        for label, f in e["feeds"].items():
            for metric in ("delay", "transport", "pub_lag", "proc"):
                r = row("feed", mk, label, metric, f.get(metric), f"msgs_per_s={f['msgs_per_s']:.3f}")
                if r:
                    yield r
        if "race" in e:
            for src, x in e["race"]["sources"].items():
                r = row("race", mk, src, "lag", x["lag"], f"share={x['share']:.4f};earliest={x['earliest_share']:.4f}")
                if r:
                    yield r
            for metric in ("pair_gain", "pair_delay_a", "pair_delay_ab"):
                r = row("race", mk, "d1_pair", metric, e["race"][metric])
                if r:
                    yield r
            for metric in ("touch_age", "hold_ts", "hold_arrival"):
                r = row("merged" if metric == "touch_age" else "hold", mk, "", metric, e.get(metric))
                if r:
                    yield r
    for t in s["network"]["tcp"]:
        yield ["network", t["host"], t["ip"], "tcp_connect", t["n"], None, None, t["median_ms"], None, None, None,
               f"min={t['min_ms']}"]
    for mk, o in s["venue_offset"].items():
        yield ["clock", mk, "", "venue_offset", None, None, None, None, None, None, None,
               f"extended_ms={o['extended_ms']};binance_ms={o['binance_ms']}"]
    for c in s["checks"]:
        yield ["check", "", c["name"], "ok" if c["ok"] else "FAIL", None, None, None, None, None, None, None, c["detail"]]


def console_summary(s: dict) -> str:
    r = s["run"]
    lines = []
    if s["clock"].get("ok_at_start") is False:
        lines.append("!!! CLOCK NOT CONVERGED !!!")
    lines.append(f"=== Extended latency {r.get('mode')} run {r['run_id']} — {_f(r['duration_s'] / 60)} min on "
                 f"{r['host'].get('instance_type') or r['host'].get('hostname')} {r['host'].get('az_id') or ''} ===")
    if r.get("mode") == "test":
        lines.append("15-minute sample: p99 and max are indicative only; tail estimates need the 24 h run.")
    for w in s["warnings"]:
        lines.append(f"WARNING: {w}")
    for mk, e in s["markets"].items():
        if mk == "ALL":
            continue
        race = (e.get("race") or {}).get("sources", {})
        lines.append("")
        lines.append(f"--- {mk} (delay ms; race lag vs earliest source) ---")
        lines.append(f"{'feed':<15}{'msg/s':>8}{'p01':>8}{'p50':>8}{'p90':>8}{'p99':>8}{'trans50':>9}"
                     f"{'race50':>8}{'race90':>8}{'shown%':>8}")
        labels = list(e["feeds"]) + [x for x in race if x not in e["feeds"]]
        for label in sorted(labels, key=_order):
            f = e["feeds"].get(label, {})
            d = f.get("delay") or {}
            tr = (f.get("transport") or {}).get("p50")
            rc = race.get(label)
            lines.append(
                f"{label:<15}{_f(f.get('msgs_per_s'), 2):>8}{_f(d.get('p01')):>8}{_f(d.get('p50')):>8}"
                f"{_f(d.get('p90')):>8}{_f(d.get('p99')):>8}{_f(tr):>9}"
                f"{_f(rc['lag']['p50'] if rc else None, 2):>8}{_f(rc['lag']['p90'] if rc else None, 2):>8}"
                f"{_f(rc['share'] * 100 if rc and rc['share'] is not None else None):>8}")
        if e.get("hold_ts"):
            lines.append(f"full-book batch hold (v2 ts − v1 ts): p50 {_f(e['hold_ts']['p50'])} p90 "
                         f"{_f(e['hold_ts']['p90'])} ms (n={e['hold_ts']['n']}); arrival diff p50 "
                         f"{_f((e.get('hold_arrival') or {}).get('p50'))} ms")
        g = (e.get("race") or {}).get("pair_gain")
        if g and g.get("n"):
            lines.append(f"second depth=1 socket gain a−min(a,b): p50 {_f(g['p50'], 2)} p90 {_f(g['p90'], 2)} "
                         f"p99 {_f(g['p99'], 2)} ms")
    lines.append("")
    lines.append("min TCP RTT per IP: " + "; ".join(f"{t['ip']} ({t['host'].split('.')[0]}) {_f(t['min_ms'], 3)}"
                                                    for t in s["network"]["tcp"]) or "-")
    lines.append("venue clock offset (ms): " + "; ".join(
        f"{mk} ext {_f(o['extended_ms'], 2)} bin {_f(o['binance_ms'], 2)}" for mk, o in s["venue_offset"].items()))
    c = s["clock"]
    lines.append(f"clock: {', '.join(c['sources'])} bound p50 {_f(c['bound_p50_ms'], 3)} max {_f(c['bound_max_ms'], 3)} ms")
    p = s["processing"]
    lines.append(f"processing recv→merged: p50 {_f((p['proc'] or {}).get('p50'), 3)} p99 "
                 f"{_f((p['proc'] or {}).get('p99'), 3)} ms; loop lag p99 {_f(p['loop_lag']['p99_median_ms'], 3)} ms")
    co = s["continuity"]
    lines.append(f"seq gaps {sum(co['gaps'].values())}, reconnects {sum(co['reconnects'].values())}, "
                 f"silent feeds {len(co['silent'])}, writer drops {co['writer_drops']}")
    lines.append("")
    for ck in s["checks"]:
        lines.append(f"[{'PASS' if ck['ok'] else 'FAIL'}] {ck['name']}: {ck['detail']}")
    ok = all(ck["ok"] for ck in s["checks"])
    lines.append("RESULT: " + ("all continuity checks passed" if ok else "CHECKS FAILED"))
    return "\n".join(lines)


def build_report(run_dir: Path, out_dir: Path | None = None, out_md: Path | None = None) -> dict:
    run_dir = Path(run_dir)
    out_dir = Path(out_dir) if out_dir else run_dir
    s = analyse(run_dir)
    md = render_md(s)
    (Path(out_md) if out_md else out_dir / "report.md").write_text(md)
    with open(out_dir / "summary.csv", "w", newline="") as f:
        csv.writer(f).writerows(csv_rows(s))
    (out_dir / "summary.json").write_bytes(orjson.dumps(s, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS))
    return s


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="analysis.report", description=__doc__.split("\n")[0])
    ap.add_argument("--data", required=True, help="run folder (data/<run-id>)")
    ap.add_argument("--out", help="report.md path (default: <data>/report.md); csv/json go next to it")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    out_md = Path(a.out) if a.out else None
    s = build_report(Path(a.data), out_dir=out_md.parent if out_md else None, out_md=out_md)
    if not a.quiet:
        print(console_summary(s))
    return 0 if all(c["ok"] for c in s["checks"]) else 2


if __name__ == "__main__":
    sys.exit(main())
