"""End-to-end: run `tracker.main --test` against the local mock venue (tests/mock_venue.py).

Covers SPEC §5.10 test-mode acceptance locally: a healthy run exits 0 and writes report.md / summary.csv /
summary.json; a deliberately muted feed makes it exit 2.
"""

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
pytestmark = pytest.mark.e2e


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def venue():
    from tests import mock_venue

    port = _free_port()
    ready = threading.Event()

    def run():
        loop = asyncio.new_event_loop()
        ev = asyncio.Event()

        async def main():
            t = asyncio.create_task(mock_venue.run(port, ev, resnap_s=8))
            await ev.wait()
            ready.set()
            await t

        loop.run_until_complete(main())

    threading.Thread(target=run, daemon=True).start()
    assert ready.wait(10)
    return port


def _cfg(tmp_path: Path, port: int, extra: str = "") -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(f"""
markets:
  - {{market: HYPE-USD, binance: hypeusdt}}
  - {{market: BTC-USD, binance: btcusdt}}
endpoints:
  rest: http://127.0.0.1:{port}/api/v1
  ws_v1: ws://127.0.0.1:{port}/v1
  ws_v2: ws://127.0.0.1:{port}/v2/rpc
  binance_ws: ws://127.0.0.1:{port}/binance/ws
warmup_s: 3
silent_book_s: 5
clock: {{mode: none}}
output_dir: {tmp_path / 'data'}
{extra}
""")
    return p


def _run(cfg: Path, run_id: str, minutes: float = 0.5):
    env = dict(os.environ, NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
    return subprocess.run([sys.executable, "-m", "tracker.main", "--config", str(cfg), "--test",
                           "--test-minutes", str(minutes), "--run-id", run_id],
                          cwd=ROOT, env=env, capture_output=True, text=True, timeout=240)


def test_healthy_run_exits_0(tmp_path, venue):
    cfg = _cfg(tmp_path, venue)
    r = _run(cfg, "ok")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    run = tmp_path / "data" / "ok"
    for f in ("report.md", "summary.csv", "summary.json", "summary.txt", "meta.jsonl", "minutes.jsonl"):
        assert (run / f).exists(), f
    s = json.loads((run / "summary.json").read_text())
    assert all(c["ok"] for c in s["checks"])
    hype = s["markets"]["HYPE-USD"]
    for label in ("ext_d1_a", "ext_d1_b", "ext_full", "ext_trades", "ext_rpc.d1", "ext_rpc.full", "ext_rpc_d1",
                  "bin_top", "ctl_all_d1", "ctl_mark"):
        assert hype["feeds"][label]["msgs"] > 0, label
    assert hype["feeds"]["ext_d1_a"]["delay"]["n"] > 100
    assert hype["feeds"]["ext_rpc.d1"]["transport"]["n"] > 100
    assert hype["race"]["states"] > 100
    assert hype["hold_ts"]["n"] > 10
    assert hype["feeds"]["ext_full"]["replay_exact"] >= 1
    assert s["continuity"]["catchup_rows"]["ext_d1_a:HYPE-USD"] >= 20  # mock replays 20 old messages
    md = (run / "report.md").read_text()
    for section in ("5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7", "5.8", "US baseline"):
        assert section in md
    assert "15-minute sample" in md
    assert "RESULT: all continuity checks passed" in r.stdout


def test_muted_feed_exits_2(tmp_path, venue):
    extra = f'url_overrides: {{ext_d1_b: "ws://127.0.0.1:{venue}/v1/orderbooks/NOPE-USD?depth=1"}}'
    cfg = _cfg(tmp_path, venue, extra)
    r = _run(cfg, "muted", minutes=0.3)
    assert r.returncode == 2, r.stdout[-3000:]
    assert "SILENT FEED ext_d1_b" in r.stdout
    s = json.loads((tmp_path / "data" / "muted" / "summary.json").read_text())
    failed = {c["name"] for c in s["checks"] if not c["ok"]}
    assert {"all_feeds_delivered", "no_silent_book_feed"} <= failed


def test_report_cli_reproducible(tmp_path, venue):
    cfg = _cfg(tmp_path, venue)
    assert _run(cfg, "rep", minutes=0.25).returncode in (0, 2)
    run = tmp_path / "data" / "rep"
    first = json.loads((run / "summary.json").read_text())
    out = tmp_path / "again" / "report.md"
    out.parent.mkdir()
    r = subprocess.run([sys.executable, "-m", "analysis.report", "--data", str(run), "--out", str(out), "--quiet"],
                       cwd=ROOT, capture_output=True, text=True)
    assert r.returncode in (0, 2), r.stderr
    again = json.loads((out.parent / "summary.json").read_text())
    assert again["markets"]["HYPE-USD"]["feeds"]["ext_d1_a"]["delay"] == \
        first["markets"]["HYPE-USD"]["feeds"]["ext_d1_a"]["delay"]
