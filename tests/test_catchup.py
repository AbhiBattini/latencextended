import random

from tracker.book import CatchupDetector

MS = 1_000_000


def test_replay_then_live():
    """Venue replays 3 s of buffer in 60 ms (venue clock outruns ours), then streams live with jitter."""
    det = CatchupDetector()
    rng = random.Random(1)
    flags = []
    mono = 0
    ts = 1_000_000
    for _ in range(30):  # replay: 100 ms of venue time per 2 ms of ours
        flags.append(("replay", det.update(ts, mono)))
        ts += 100
        mono += 2 * MS
    for _ in range(400):  # live: 10 ms per 10 ms, +-4 ms jitter on arrival
        flags.append(("live", det.update(ts, mono + rng.randint(-4, 4) * MS)))
        ts += 10
        mono += 10 * MS
    replay = [f for k, f in flags if k == "replay"]
    live = [f for k, f in flags if k == "live"]
    assert all(replay), "every replay row must be flagged"
    # flagged until excess stayed flat for 1.5 s, then sticky-false
    first_live = live.index(False)
    assert 140 <= first_live <= 160
    assert not any(live[first_live:])


def test_no_replay_only_hold_window():
    det = CatchupDetector()
    flags = [det.update(1000 + i * 10, i * 10 * MS) for i in range(300)]
    assert flags.index(False) == 150  # 1.5 s at 10 ms spacing
    assert not any(flags[150:])


def test_jitter_downwards_does_not_reset():
    det = CatchupDetector()
    mono = 0
    ts = 0
    out = []
    for i in range(400):
        # each message arrives later and later relative to the venue (growing delay -> excess falls)
        out.append(det.update(ts, mono + i * MS // 10))
        ts += 10
        mono += 10 * MS
    assert 148 <= out.index(False) <= 150  # never reset by downward moves


def test_max_cap():
    det = CatchupDetector(max_s=2.0)
    # pathological: venue clock always faster -> would never settle
    flags = [det.update(i * 100, i * 10 * MS) for i in range(400)]
    assert flags.index(False) == 200
