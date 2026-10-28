import json
from pathlib import Path

from tracker.book import MergedBook, OrderBook, touch_from_levels
from tracker.engine import Engine, label_for
from tracker.feeds import V1_D1, V1_FULL, V1_TRADES, V2, header

FX = Path(__file__).parent / "fixtures"


def fx(name):
    return json.loads((FX / name).read_text())


def lv(p, q, c=None):
    d = {"p": str(p), "q": str(q)}
    if c is not None:
        d["c"] = str(c)
    return d


def test_snapshot_then_delta_applies_c_not_q():
    b = OrderBook()
    snap = fx("v1_full_snapshot.json")
    assert b.apply(True, snap["data"], snap["ts"]) is None  # first snapshot is not a replay check
    assert b.top() == (80.827, 81.26, 80.830, 5.0)
    d = fx("v1_full_delta.json")
    b.apply(False, d["data"], d["ts"])
    # 80.827 deleted via c == "0"; 80.815 set to c (17.11), not accumulated from q
    assert 80.827 not in b.bids
    assert b.bids[80.815] == 17.11
    assert b.top()[:2] == (80.826, 14.63)
    assert b.last_ts == d["ts"]


def test_delta_sets_resulting_quantity():
    b = OrderBook()
    b.apply(True, {"b": [lv(10, 5)], "a": [lv(11, 5)]}, 1)
    b.apply(False, {"b": [lv(10, -2, 3)], "a": []}, 2)  # q is the signed change, c the result
    assert b.bids[10.0] == 3.0
    b.apply(False, {"b": [lv(10, 4, 7)], "a": []}, 3)
    assert b.bids[10.0] == 7.0


def test_best_tracking_on_delete_and_improve():
    b = OrderBook()
    b.apply(True, {"b": [lv(10, 1), lv(9, 1), lv(8, 1)], "a": [lv(11, 1), lv(12, 1)]}, 1)
    b.apply(False, {"b": [lv(10, -1, 0)], "a": [lv(10.5, 2, 2)]}, 2)
    assert b.best_bid == 9.0 and b.best_ask == 10.5
    b.apply(False, {"b": [lv(9.5, 1, 1)], "a": [lv(10.5, -2, 0)]}, 3)
    assert b.best_bid == 9.5 and b.best_ask == 11.0


def test_snapshot_replaces_book_and_checks_replay():
    b = OrderBook()
    b.apply(True, {"b": [lv(10, 1)], "a": [lv(11, 1)]}, 1)
    b.apply(False, {"b": [lv(9, 2, 2)], "a": []}, 2)
    assert b.apply(True, {"b": [lv(10, 1), lv(9, 2)], "a": [lv(11, 1)]}, 3) is True
    assert b.apply(True, {"b": [lv(10, 1)], "a": [lv(12, 1)]}, 4) is False  # mismatch detected ...
    assert b.bids == {10.0: 1.0} and b.asks == {12.0: 1.0}  # ... and the snapshot replaced the book


def test_delta_before_snapshot_ignored():
    b = OrderBook()
    b.apply(False, {"b": [lv(10, 1, 1)], "a": []}, 1)
    assert not b.valid and b.top() is None


def test_seq_gap_resets_book_on_new_connection():
    from tracker.live import LiveSink

    e = Engine(["HYPE-USD"], LiveSink())
    snap = fx("v1_full_snapshot.json")
    e.process("ext_full", V1_FULL, "HYPE-USD", 1, 1, snap, False)
    assert e.full["HYPE-USD"].valid
    # a seq gap closes the socket; the next connection starts fresh and must not apply deltas until a snapshot
    d = fx("v1_full_delta.json")
    e.process("ext_full", V1_FULL, "HYPE-USD", 2, 2, d, False)
    assert not e.full["HYPE-USD"].valid
    e.process("ext_full", V1_FULL, "HYPE-USD", 2, 3, snap, False)
    assert e.full["HYPE-USD"].valid


def test_merged_d1_overrides_and_crossed_levels_hidden():
    full = OrderBook()
    full.apply(True, {"b": [lv(10.0, 1), lv(9.9, 2)], "a": [lv(10.1, 1), lv(10.2, 3)]}, 100)
    mb = MergedBook("X", full)
    mb.on_full(100)
    assert mb.state() == (10.0, 1.0, 10.1, 1.0) and mb.src == "ext_full"
    # depth=1 newer than full: ask moved down to 10.05, bid 10.0 lifted to 10.02
    assert mb.on_d1("ext_d1_a", 105, (10.02, 4.0, 10.05, 2.0))
    assert mb.src == "ext_d1_a" and mb.src_ts == 105
    bids, asks = mb.view(5)
    assert bids[0] == (10.02, 4.0) and all(p < 10.02 for p, _ in bids[1:])
    assert asks[0] == (10.05, 2.0) and all(p > 10.05 for p, _ in asks[1:])
    assert full.best_bid == 10.0  # stored book untouched
    # duplicate from the second socket: no change, first arrival kept
    assert not mb.on_d1("ext_d1_b", 105, (10.02, 4.0, 10.05, 2.0))
    assert mb.src == "ext_d1_a"
    # older depth=1 from the slower socket never regresses the touch
    assert not mb.on_d1("ext_d1_b", 103, (10.0, 1.0, 10.1, 1.0))
    # a full message older than the d1 touch does not override it; a newer one does
    full.apply(False, {"b": [], "a": [lv(10.1, 0, 0)]}, 104)
    assert not mb.on_full(104)
    full.apply(False, {"b": [lv(10.02, 4, 4)], "a": [lv(10.05, 2, 2)]}, 110)
    mb.on_full(110)
    assert mb.src == "ext_full"


def test_d1_older_than_full_rejected():
    full = OrderBook()
    full.apply(True, {"b": [lv(10, 1)], "a": [lv(11, 1)]}, 200)
    mb = MergedBook("X", full)
    mb.on_full(200)
    assert not mb.on_d1("ext_d1_a", 150, (10, 2, 11, 2))
    assert mb.src == "ext_full"


def test_provisional_trade_update():
    full = OrderBook()
    full.apply(True, {"b": [lv(10.0, 1), lv(9.9, 2)], "a": [lv(10.1, 1.5), lv(10.2, 3)]}, 100)
    mb = MergedBook("X", full)
    mb.on_full(100)
    # aggressor BUY eats asks
    assert mb.on_trade("BUY", 10.1, 0.5, 101)
    assert mb.state() == (10.0, 1.0, 10.1, 1.0) and mb.src == "trade_prov"
    assert mb.on_trade("BUY", 10.1, 1.0, 102)  # level consumed -> next level from the full book
    assert mb.state() == (10.0, 1.0, 10.2, 3.0)
    # same-ms trade as the touch: the book already shows it (strictly-before rule)
    assert not mb.on_trade("SELL", 10.0, 0.5, 102)
    assert mb.on_trade("SELL", 10.0, 0.4, 103)
    assert mb.state()[:2] == (10.0, 0.6)
    # next depth=1 overwrites the provisional state
    assert mb.on_d1("ext_d1_a", 104, (10.0, 0.6, 10.2, 2.0))
    assert mb.src == "ext_d1_a"


def test_headers_and_labels():
    assert header(V1_D1, fx("v1_d1.json"))[:2] == (1793213195514, 2)
    ts, seq, T, topic, m = header(V1_TRADES, fx("v1_trades.json"))
    assert (ts, T, m) == (1793213196699, 1793213196689, "HYPE-USD")
    ts, seq, T, topic, m = header(V2, fx("v2_d1.json"))
    assert seq == 0 and topic == "orderbooks.1.HYPE-USD"
    assert label_for("ext_rpc", V2, fx("v2_d1.json")) == ("ext_rpc.d1", "d1")
    assert label_for("ext_rpc_d1", V2, fx("v2_d1.json")) == ("ext_rpc_d1", "d1")
    assert label_for("ext_rpc", V2, fx("v2_ack.json"))[1] is None
    assert touch_from_levels(fx("v1_d1.json")["data"]) == (80.653, 74.55, 80.654, 288.97)


def test_engine_delays_and_race():
    from tracker.live import LiveSink

    sink = LiveSink()
    e = Engine(["HYPE-USD"], sink, race_window_s=1)
    d1 = fx("v1_d1.json")
    t = d1["ts"] * 1_000_000
    e.process("ext_d1_a", V1_D1, "HYPE-USD", 1, t + 10_000_000, d1, False)
    e.process("ext_d1_b", V1_D1, "HYPE-USD", 1, t + 12_000_000, d1, False)
    e.process("ext_rpc", V2, "HYPE-USD", 1, t + 11_000_000, fx("v2_d1.json"), False)
    e.process("ext_d1_a", V1_D1, "HYPE-USD", 1, t + 5_000_000_000, {**d1, "ts": d1["ts"] + 4000}, True)  # excluded
    assert sink.values["delay"][("HYPE-USD", "ext_d1_a")] == [10.0]
    assert sink.values["transport"][("HYPE-USD", "ext_rpc.d1")] == [8.0]
    e.race["HYPE-USD"].expire(None)
    rs = e.race["HYPE-USD"].summary()
    assert rs["states"] == 1
    assert rs["sources"]["ext_d1_a"]["earliest_share"] == 1.0
    assert rs["sources"]["ext_d1_b"]["lag"]["p50"] == 2.0
    assert rs["sources"]["ext_rpc.d1"]["lag"]["p50"] == 1.0
    assert rs["pair_gain"]["p50"] == 0.0
