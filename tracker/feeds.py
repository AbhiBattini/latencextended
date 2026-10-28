"""Feed definitions (SPEC §3.4) and the light header parse done in the receive path."""

from __future__ import annotations

from dataclasses import dataclass, field

# kinds
V1_D1 = "v1_d1"
V1_FULL = "v1_full"
V1_TRADES = "v1_trades"
V1_PRICE = "v1_price"
V2 = "v2"
BIN = "bin_book"

FEED_KINDS = {
    "ext_d1_a": V1_D1, "ext_d1_b": V1_D1, "ctl_all_d1": V1_D1,
    "ext_full": V1_FULL, "ctl_all_full": V1_FULL, "ext_rfq": V1_FULL,
    "ext_trades": V1_TRADES, "ctl_mark": V1_PRICE, "ctl_index": V1_PRICE,
    "ext_rpc": V2, "ext_rpc_d1": V2, "bin_top": BIN,
}
ALL_MARKET_FEEDS = ("ctl_all_d1", "ctl_all_full")

PRODUCTION = ("ext_d1_a", "ext_d1_b", "ext_full", "ext_trades", "ext_rpc", "ext_rpc_d1", "bin_top")


@dataclass
class FeedSpec:
    name: str
    kind: str
    market: str | None  # None = all-markets stream
    url: str
    subscribe: list = field(default_factory=list)
    silent_s: float = 30.0
    reconnect_on_gap: bool = False
    seq_check: bool = True
    control: bool = False
    is_book: bool = True

    @property
    def key(self) -> str:
        return f"{self.name}:{self.market or 'ALL'}"


def _rpc_sub(i: int, scope: str, selector: dict) -> dict:
    return {"jsonrpc": "2.0", "id": i, "method": "subscribe", "params": {"scope": scope, "selector": selector}}


def build_feeds(cfg: dict, controls: bool, only: set[str] | None = None) -> list[FeedSpec]:
    ep = cfg["endpoints"]
    v1, v2, bws = ep["ws_v1"].rstrip("/"), ep["ws_v2"], ep["binance_ws"].rstrip("/")
    on = cfg["feeds"]
    over = cfg.get("url_overrides") or {}
    book_s, trade_s = cfg["silent_book_s"], cfg["silent_trades_s"]
    out: list[FeedSpec] = []

    def want(name: str, default: bool) -> bool:
        if only is not None:
            return name in only
        return bool(on.get(name, default))

    def url(name: str, default: str, market: str | None) -> str:
        u = over.get(name, default)
        return u.replace("{market}", market or "")

    for m in cfg["markets"]:
        mk, sym = m["market"], (m.get("binance") or "").lower()
        if want("ext_d1_a", True):
            out.append(FeedSpec("ext_d1_a", V1_D1, mk, url("ext_d1_a", f"{v1}/orderbooks/{mk}?depth=1", mk), silent_s=book_s))
        if want("ext_d1_b", True):
            out.append(FeedSpec("ext_d1_b", V1_D1, mk, url("ext_d1_b", f"{v1}/orderbooks/{mk}?depth=1", mk), silent_s=book_s))
        if want("ext_full", True):
            out.append(FeedSpec("ext_full", V1_FULL, mk, url("ext_full", f"{v1}/orderbooks/{mk}", mk), silent_s=book_s, reconnect_on_gap=True))
        if want("ext_trades", True):
            out.append(FeedSpec("ext_trades", V1_TRADES, mk, url("ext_trades", f"{v1}/publicTrades/{mk}", mk), silent_s=trade_s, is_book=False))
        if want("ext_rpc", True):
            subs = [
                _rpc_sub(1, "orderbooks", {"market": mk, "depth": "1", "rfqOnly": False}),
                _rpc_sub(2, "orderbooks", {"market": mk, "depth": "full", "rfqOnly": False}),
                _rpc_sub(3, "trades", {"market": mk}),
            ]
            out.append(FeedSpec("ext_rpc", V2, mk, url("ext_rpc", v2, mk), subs, silent_s=book_s, reconnect_on_gap=True))
        if want("ext_rpc_d1", True):
            subs = [_rpc_sub(1, "orderbooks", {"market": mk, "depth": "1", "rfqOnly": False})]
            out.append(FeedSpec("ext_rpc_d1", V2, mk, url("ext_rpc_d1", v2, mk), subs, silent_s=book_s))
        if sym and want("bin_top", True):
            out.append(FeedSpec("bin_top", BIN, mk, url("bin_top", f"{bws}/{sym}@bookTicker", mk), silent_s=book_s, seq_check=False))
        if controls or (only and "ctl_mark" in only):
            out.append(FeedSpec("ctl_mark", V1_PRICE, mk, url("ctl_mark", f"{v1}/prices/mark/{mk}", mk), silent_s=book_s, control=True, is_book=False))
        if controls or (only and "ctl_index" in only):
            out.append(FeedSpec("ctl_index", V1_PRICE, mk, url("ctl_index", f"{v1}/prices/index/{mk}", mk), silent_s=book_s, control=True, is_book=False))
    if controls or (only and "ctl_all_d1" in only):
        out.append(FeedSpec("ctl_all_d1", V1_D1, None, url("ctl_all_d1", f"{v1}/orderbooks?depth=1", None), silent_s=book_s, control=True))
    if controls or (only and "ctl_all_full" in only):
        # all-markets full depth: per-market seq/book handling is not attempted; delay statistics only
        out.append(FeedSpec("ctl_all_full", V1_FULL, None, url("ctl_all_full", f"{v1}/orderbooks", None), silent_s=book_s, control=True))
    return out


def rfq_feed(cfg: dict, market: str) -> FeedSpec:
    v1 = cfg["endpoints"]["ws_v1"].rstrip("/")
    return FeedSpec("ext_rfq", V1_FULL, market, f"{v1}/orderbooks/rfq/{market}", silent_s=cfg["silent_trades_s"], reconnect_on_gap=True)


def header(kind: str, msg) -> tuple:
    """(venue_ts_ms, seq, match_T_ms, topic, market) from a parsed message. Cheap: no level parsing."""
    if kind == BIN:
        return msg.get("E"), msg.get("u"), None, "", None
    if not isinstance(msg, dict):
        return None, None, None, "", None
    data = msg.get("data")
    T = None
    m = None
    if isinstance(data, list):
        if data:
            T = max((t.get("T") or 0) for t in data) or None
            m = data[0].get("m")
    elif isinstance(data, dict):
        m = data.get("m")
    return msg.get("ts"), msg.get("seq"), T, msg.get("subscription") or "", m
