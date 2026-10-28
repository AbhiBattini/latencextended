import numpy as np

from tracker.stats import Histogram, pcts


def test_histogram_matches_numpy():
    rng = np.random.default_rng(3)
    vals = np.concatenate([rng.normal(10, 3, 50_000), rng.lognormal(5, 1, 2_000), rng.normal(-2, 0.5, 1000)])
    h = Histogram()
    for v in vals:
        h.add(float(v))
    q = h.quantiles((0.01, 0.5, 0.9, 0.99))
    ref = np.percentile(vals, [1, 50, 90, 99])
    for got, want in zip(q, ref):
        assert abs(got - want) <= max(0.02, abs(want) * 0.005), (got, want)
    s = h.summary()
    assert s["n"] == len(vals) and s["max"] == vals.max() and s["min"] == vals.min()


def test_histogram_merge_and_empty():
    a, b = Histogram(), Histogram()
    assert a.summary()["p50"] is None
    for v in range(100):
        a.add(v)
        b.add(v + 100)
    a.merge(b)
    assert a.n == 200 and abs(a.quantiles((0.5,))[0] - 99.5) < 1


def test_pcts():
    d = pcts([1, 2, 3, 4, 5])
    assert d["p50"] == 3 and d["n"] == 5 and d["max"] == 5
    assert pcts([])["n"] == 0
