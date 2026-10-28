"""Config loading: config.yaml merged over built-in defaults."""

from __future__ import annotations

import copy
from pathlib import Path

import yaml

DEFAULTS: dict = {
    "markets": [
        {"market": "HYPE-USD", "binance": "hypeusdt"},
        {"market": "BTC-USD", "binance": "btcusdt"},
        {"market": "ETH-USD", "binance": "ethusdt"},
        {"market": "SOL-USD", "binance": "solusdt"},
    ],
    "endpoints": {
        "rest": "https://api.starknet.extended.exchange/api/v1",
        "ws_v1": "wss://api.starknet.extended.exchange/stream.extended.exchange/v1",
        "ws_v2": "wss://api.starknet.extended.exchange/stream.extended.exchange/v2/rpc",
        "binance_ws": "wss://fstream.binance.com/ws",
    },
    "user_agent": "ext-latency-tracker/0.1",
    "feeds": {
        "ext_d1_a": True,
        "ext_d1_b": True,
        "ext_full": True,
        "ext_trades": True,
        "ext_rpc": True,
        "ext_rpc_d1": True,
        "bin_top": True,
    },
    "controls": False,
    "url_overrides": {},
    "output_dir": "data",
    "s3_bucket": None,
    "run_minutes": 60,
    "test_minutes": 15,
    "warmup_s": 15,
    "silent_book_s": 30,
    "silent_trades_s": 900,
    "netprobe_interval_s": 60,
    "markets_poll_s": 600,
    "race_window_s": 5,
    "writer_queue": 500000,
    "clock": {
        "mode": "auto",
        "max_error_ms": 1.0,
        "wait_s": 60,
        "sample_s": 60,
        "sntp_servers": ["time.google.com", "time.cloudflare.com"],
    },
}


def _merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def load(path: str | Path | None) -> dict:
    data = {}
    if path and Path(path).exists():
        data = yaml.safe_load(Path(path).read_text()) or {}
    cfg = _merge(DEFAULTS, data)
    cfg["markets"] = [m if isinstance(m, dict) else {"market": m, "binance": ""} for m in cfg["markets"]]
    return cfg


def binance_map(cfg: dict) -> dict[str, str]:
    """Upper-case Binance symbol -> Extended market."""
    return {m["binance"].upper(): m["market"] for m in cfg["markets"] if m.get("binance")}
