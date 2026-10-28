"""Streaming percentiles.

`Histogram` is a mergeable log-linear histogram for signed millisecond values: 10 µs buckets inside ±100 ms,
0.2 % relative buckets beyond. Memory is bounded by the number of distinct buckets, so a 24 h run with tens of
millions of samples costs a few hundred kB per series. `pcts` is the exact version for small per-minute lists.
"""

from __future__ import annotations

import math

import numpy as np

QS = (0.01, 0.10, 0.50, 0.90, 0.99)
QNAMES = ("p01", "p10", "p50", "p90", "p99")

_LIN = 100.0
_LIN_SCALE = 100  # buckets per ms inside the linear range
_LIN_KEYS = int(_LIN * _LIN_SCALE)
_LOG_BASE = math.log(1.002)


def _key(v: float) -> int:
    a = abs(v)
    if a < _LIN:
        return int(round(v * _LIN_SCALE))
    k = _LIN_KEYS + 1 + int(math.log(a / _LIN) / _LOG_BASE)
    return k if v >= 0 else -k


def _value(k: int) -> float:
    a = abs(k)
    if a <= _LIN_KEYS:
        return k / _LIN_SCALE
    v = _LIN * math.exp((a - _LIN_KEYS - 1 + 0.5) * _LOG_BASE)
    return v if k >= 0 else -v


class Histogram:
    __slots__ = ("counts", "n", "min", "max", "total")

    def __init__(self) -> None:
        self.counts: dict[int, int] = {}
        self.n = 0
        self.min = math.inf
        self.max = -math.inf
        self.total = 0.0

    def add(self, v: float) -> None:
        k = _key(v)
        self.counts[k] = self.counts.get(k, 0) + 1
        self.n += 1
        self.total += v
        if v < self.min:
            self.min = v
        if v > self.max:
            self.max = v

    def merge(self, other: "Histogram") -> None:
        for k, c in other.counts.items():
            self.counts[k] = self.counts.get(k, 0) + c
        self.n += other.n
        self.total += other.total
        self.min = min(self.min, other.min)
        self.max = max(self.max, other.max)

    def quantiles(self, qs=QS) -> list[float | None]:
        if not self.n:
            return [None] * len(qs)
        keys = sorted(self.counts)
        out = []
        targets = sorted((q, i) for i, q in enumerate(qs))
        res = [None] * len(qs)
        cum = 0
        ti = 0
        for k in keys:
            cum += self.counts[k]
            while ti < len(targets) and cum > targets[ti][0] * (self.n - 1):
                res[targets[ti][1]] = min(max(_value(k), self.min), self.max)
                ti += 1
            if ti == len(targets):
                break
        while ti < len(targets):
            res[targets[ti][1]] = self.max
            ti += 1
        out.extend(res)
        return out

    def summary(self) -> dict:
        d = dict(zip(QNAMES, self.quantiles()))
        d["n"] = self.n
        d["min"] = self.min if self.n else None
        d["max"] = self.max if self.n else None
        d["mean"] = self.total / self.n if self.n else None
        return d


def pcts(values, qs=QS) -> dict:
    """Exact percentiles of a small list (per-minute summaries)."""
    if len(values) == 0:
        return {"n": 0, **{q: None for q in QNAMES}, "max": None}
    a = np.asarray(values, dtype=float)
    ps = np.percentile(a, [q * 100 for q in qs])
    d = {name: float(p) for name, p in zip(QNAMES, ps)}
    d["n"] = int(a.size)
    d["max"] = float(a.max())
    return d
