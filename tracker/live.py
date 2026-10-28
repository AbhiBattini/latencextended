"""Live statistics sink and the per-minute summary record (SPEC §4.4)."""

from __future__ import annotations

from collections import defaultdict

from .stats import pcts


class LiveSink:
    def __init__(self) -> None:
        self._new()

    def _new(self) -> None:
        self.values: dict = defaultdict(lambda: defaultdict(list))
        self.counts: dict = defaultdict(lambda: defaultdict(int))

    def add(self, metric: str, key, value: float) -> None:
        self.values[metric][key].append(value)

    def count(self, metric: str, key, n: int = 1) -> None:
        self.counts[metric][key] += n

    def swap(self):
        v, c = self.values, self.counts
        self._new()
        return v, c


def _k(key) -> str:
    return "|".join(str(x) for x in key) if isinstance(key, tuple) else str(key)


def build_minute(minute_start_ns: int, values, counts, conns, race_lags, loop_lags, clock, rtt, writer_drops) -> dict:
    """Pure function (runs in a worker thread)."""
    out = {"type": "minute", "minute_wall_ns": minute_start_ns, "loop_lag": pcts(loop_lags),
           "clock": clock, "rtt": rtt, "writer_drops": writer_drops, "feeds": {}, "metrics": {}, "race": {}}
    for c in conns:
        out["feeds"][c["key"]] = c
    for metric, by_key in values.items():
        out["metrics"][metric] = {_k(k): pcts(v) for k, v in by_key.items()}
    for metric, by_key in counts.items():
        out["metrics"].setdefault(metric, {})
        for k, n in by_key.items():
            out["metrics"][metric][_k(k)] = {"count": n}
    for mk, by_src in race_lags.items():
        out["race"][mk] = {src: pcts(v) for src, v in by_src.items()}
    return out
