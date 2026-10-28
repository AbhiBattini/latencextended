# Extended Exchange Feed Latency Tracker — Build Specification

**Audience:** an engineer or coding agent with no prior context. Everything you need is in this file.
**Goal:** build a small, self-contained service that subscribes to Extended's public market-data feeds the way a
low-latency market maker would, measures how late each piece of information reaches us, and runs on an AWS EC2
instance in Tokyo — the same AWS zone as Extended's matching engine — so we get the **co-located** numbers.

Everything below marked **(measured)** was observed live on 2026-10-28 from a home machine in the US (round trip to
Tokyo ≈ 179 ms). Everything marked **(docs)** comes from Extended's API docs. Treat anything else as a
recommendation you may change if you have a better reason.

---

## 0. TL;DR

1. Extended (`extended.exchange`, a perpetual-futures exchange, ex-"X10") runs its engine in **AWS Tokyo,
   `ap-northeast-1`, availability-zone ID `apne1-az4`** (docs). Its API hosts resolve straight to AWS Tokyo IPs — no CDN.
2. The best feed set (settled by measurement, §3):
   - **Touch (best bid/ask):** `orderbooks/{market}?depth=1` — pushed every ≤10 ms, full snapshot each time. Two sockets.
   - **Depth:** `orderbooks/{market}` — full order book, snapshot + deltas, batched every 100 ms.
   - **Trades:** `publicTrades/{market}` — arrives a few ms *before* the touch feed reflects the trade.
   - **v2 JSON-RPC socket** carrying the same topics — same speed, but its `ts` is the **send** time, which lets us
     measure transport latency per message directly.
   - **Binance USD-M futures `bookTicker`** — reference feed (Binance is also in AWS Tokyo).
3. Measure, per feed: message rate, delivery delay, transport delay, the batching hold of the full book, a "touch
   race" between feeds, and how stale the merged book is. Write raw messages + per-minute summaries; produce a report.
4. Deploy with one command (`deploy/launch.sh`) that starts a `c7i.large` in `apne1-az4` with cloud-init, chrony on
   the Amazon Time Sync Service, a systemd service, no inbound ports (SSM access), results synced to S3, and an
   auto-shutdown timer.
5. **Test mode** (`--test`, §4.6): a 15-minute run that captures every feed, then immediately prints the latency
   tables and uploads `report.md`. `deploy/launch.sh --test` does the whole thing on EC2 and the instance terminates
   itself afterwards. Use it first; run the 24 h measurement only once test mode looks sane.

---

## 1. Background you need

### 1.1 What the tracker is for

A market-making strategy quotes at the best bid/ask on Extended and uses order-book features (e.g. dollar depth
within 5 bps of mid) plus a Binance reference price. Backtests showed the strategy only works with Tokyo-like
latency. From the US, every Extended message arrives ≈ 90 ms after the venue sends it. We need the real
**co-located** delays to (a) re-time the backtest honestly and (b) choose the production feed setup.

The tracker does **not** trade. It needs no API key, no account and no signing.

### 1.2 The venue in one paragraph

Extended is a perpetual-futures DEX settled on Starknet, but **matching and market data are off-chain**, served from AWS
Tokyo. Public REST base: `https://api.starknet.extended.exchange/api/v1`. Public WebSocket base:
`wss://api.starknet.extended.exchange/stream.extended.exchange/v1`. Markets are named `{ASSET}-USD` (e.g. `HYPE-USD`,
`BTC-USD`, `ETH-USD`, `SOL-USD`). Prices and quantities are **strings** in every message — parse them as decimals or
floats, never assume a fixed precision. Extended's maker fee is 0 on normal order-book markets, which is why queue
position and latency matter so much here.

### 1.3 Account eligibility note

Server location does not change who may use the exchange. Extended's terms restrict some jurisdictions. Public market
data needs no account; the tracker only reads public data.

---

## 2. Protocol reference (everything you need to parse)

### 2.1 Universal rules

| Rule | Source |
|---|---|
| **`User-Agent` header is mandatory** on REST *and* WebSocket. Without it you get rejected. Send e.g. `User-Agent: ext-latency-tracker/0.1`. | docs + measured |
| Server sends a WebSocket **ping every 15 s** and expects a pong within 10 s. Standard WS libraries answer pings automatically — make sure yours does. | docs |
| REST: 1,000 requests/minute/IP by default. The tracker barely uses REST. | docs |
| **One topic per socket on v1.** Each URL is its own connection. | docs + SDK |
| Do not use **REST `/info/markets/{m}/orderbook` as truth** — it was measured 4–87 s stale (median 43 s). | measured |
| `?depth=20` (or any value other than 1) is **rejected**. Only `depth=1` or full depth exist. | measured |
| The docs' `/trades/{market}` path returns **HTTP 404**; the working path is **`/publicTrades/{market}`**. | measured |

### 2.2 v1 order book, full depth — `GET wss://…/v1/orderbooks/{market}`

- Push frequency **100 ms** (docs). Measured ≈ 7–9 msg/s on HYPE-USD.
- First message is a `SNAPSHOT`; then `DELTA`s. The venue **re-sends a SNAPSHOT about once a minute** in-stream
  (measured: 16 snapshots in 15 min). A mid-stream SNAPSHOT replaces the whole book.
- `seq` starts at 1 and increments by **exactly 1 per message** on this connection (measured: 0 gaps in 6,361
  messages). If `seq` is not previous+1, the book is corrupt → **reconnect** (docs say the same).
- Snapshot levels are `{q, p}`. Delta levels are `{q, p, c}`: **`q` is the signed change, `c` is the resulting
  quantity at that price.** Always apply `c` (set, never accumulate). `c == "0"` deletes the level.
- `ts` (top level) is in **ms since epoch** and represents the **event time** of the change, *not* the send time:
  a full-book message arrives ~30–45 ms later than its `ts` would suggest because the venue holds changes for its
  100 ms batch (measured, §3.3).
- The book is **genuinely full depth** — thousands of levels per side on liquid markets (HYPE-USD: 3,473 bids /
  852 asks measured). Do not truncate on ingest.

```json
{"type":"SNAPSHOT","data":{"t":"SNAPSHOT","m":"HYPE-USD","b":[{"q":"81.26","p":"80.827"},{"q":"14.63","p":"80.826"}],"a":[...],"d":"f"},"ts":1793214936450,"seq":1}
{"type":"DELTA","data":{"t":"DELTA","m":"HYPE-USD","b":[{"q":"-17.11","p":"80.621","c":"0"},{"q":"17.11","p":"80.615","c":"17.11"}],"a":[],"d":"f"},"ts":1793213186470,"seq":2}
```

### 2.3 v1 best bid/ask — `GET wss://…/v1/orderbooks/{market}?depth=1`

- Push frequency **10 ms** (docs). It pushes on change only, so the measured rate was 7–18 msg/s.
- **Every message is a SNAPSHOT** of the top level (one bid, one ask). `seq` +1 per message (measured, 0 gaps).
- `data.d == "1"`.

```json
{"type":"SNAPSHOT","data":{"t":"SNAPSHOT","m":"HYPE-USD","b":[{"q":"74.55","p":"80.653"}],"a":[{"q":"288.97","p":"80.654"}],"d":"1"},"ts":1793213195514,"seq":2}
```

### 2.4 v1 trades — `GET wss://…/v1/publicTrades/{market}`

- `data` is a **list** of trades; no `type` field on v1. Each trade: `i` id, `m` market, `S` **aggressor side**
  (`BUY`/`SELL`), `tT` type (`TRADE`, `LIQUIDATION`, `DELEVERAGE`), `T` match time (ms), `p` price, `q` qty.
- `ts` = publish time. **`ts − T` ≈ 8–9 ms median, p99 40–110 ms** (measured, clock-free since both stamps are the venue's).
- **The first message after connecting is a backfill batch** of recent, old trades (measured: up to ~60 s old).
  Flag it (`is_backfill = True`) and exclude it from latency stats.
- `seq` +1 per message.

```json
{"data":[{"i":2115515594181709826,"m":"HYPE-USD","S":"SELL","tT":"TRADE","T":1793213196689,"p":"80.653","q":"0.15"}],"ts":1793213196699,"seq":2}
```

### 2.5 Other v1 streams (low value for latency, include as optional)

| Path | Notes |
|---|---|
| `/v1/orderbooks` (no market) | **All markets, full depth** on one socket — ~360 msg/s, ~450 MB/h. Same speed at the median, fat tail (p99 up to 2 s). **Do not use for trading.** Optional "control" feed. |
| `/v1/orderbooks?depth=1` | All markets, touch only — ~390 msg/s total. p99 +155 ms vs the single-market socket. Optional control. |
| `/v1/prices/mark/{m}`, `/v1/prices/index/{m}` | ~0.7 msg/s (every ~1.5 s). Not a quoting input. `type` = `MP` / `IP`. |
| `/v1/funding/{m}` | Hourly. Not needed. |
| `/v1/candles/{m}/{trades\|mark-prices\|index-prices}?interval=PT1M` | 1–10 s push. Not needed. |
| `/v1/orderbooks/rfq/{m}` | Real book for RFQ markets only; empty for normal markets. See §2.8. |

### 2.6 v2 JSON-RPC multiplexed stream — `wss://api.starknet.extended.exchange/stream.extended.exchange/v2/rpc`

Not in the public docs page; it is configured in Extended's official Python SDK (`github.com/x10xchange/python_sdk`,
`x10/config.py` → `stream_rpc_url`, protocol in `x10/clients/streamrpc/`). Measured working.

- One socket, many topics. Subscribe with JSON-RPC 2.0:

```json
{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"scope":"orderbooks","selector":{"market":"HYPE-USD","depth":"1","rfqOnly":false}}}
{"jsonrpc":"2.0","id":2,"method":"subscribe","params":{"scope":"orderbooks","selector":{"market":"HYPE-USD","depth":"full","rfqOnly":false}}}
{"jsonrpc":"2.0","id":3,"method":"subscribe","params":{"scope":"trades","selector":{"market":"HYPE-USD"}}}
```

  Other scopes in the SDK: `prices` (`{"type":"mark"|"index","market":…}`), `funding-rates`, `candles`, `account`
  (private). Methods: `subscribe`, `unsubscribe`, `ping`, `list-subscriptions`. `market: null` = all markets.
- Ack: `{"id":"1","result":{"method":"subscribe","status":"OK","subscription":"orderbooks.1.HYPE-USD"},"jsonrpc":"2.0"}`
- Data: same payloads as v1 plus a `subscription` field; `type` is `ORDERBOOKS.SNAPSHOT`, `ORDERBOOKS.DELTA` or
  `TRADES`. Topic ids: `orderbooks.1.{m}`, `orderbooks.{m}`, `trades.{m}`.
- **`seq` is one counter per CONNECTION, shared across all topics on it, starting at 0** (measured: contiguous on a
  single-topic socket; on a 3-topic socket per-topic diffs were 1, 2, 3, 4… because topics interleave). Do gap
  detection on the connection-wide sequence, not per topic.
- **`ts` on v2 is the SEND time** (measured: v2's delivery floor equals pure network; v2-full `ts` minus v1-full `ts`
  for the same book state ≈ +45 ms = the batch hold). This makes v2 the clean probe of **transport latency**.
- Speed: same as v1 at the median (touch receipt within ~1 ms of v1), full book possibly ~10 ms faster than v1
  (inside noise in one 15-min window). Worth re-measuring in Tokyo.
- The SDK sends an RPC `ping` periodically; the server also pings at the WS level.

### 2.7 Connect-time replay (important gotcha, measured)

On (re)connect, Extended's **v1 book stream replays its buffer with the ORIGINAL timestamps and carries no flag for
it** (the trades stream at least sends one obvious backfill batch). Measured on a 15-market capture: 2,905 of 19,177
book rows in the first ~10 s were replay; they produced fake multi-second "latencies".

Detect it without any clock knowledge: during replay the venue clock advances faster than your monotonic clock.
Track `excess = (venue_ts − venue_ts_first) − (mono_now − mono_first)`; it rises during replay and plateaus once live.
Mark rows `is_catchup = True` until `excess` has stayed flat (±5 ms) for 1.5 s, then make it sticky-false for that
connection. Simpler fallback: exclude the first 15 s after every connect from statistics (but still apply the
messages to the book).

### 2.8 RFQ markets and the `isRfq` flag

`GET /api/v1/info/markets` returns every market with `isRfq` (measured 2026-10-28: **340 of 419 markets are RFQ**,
including many crypto alts; HYPE/BTC/ETH/SOL are not). On an RFQ market the normal `orderbooks/{m}` stream returns an
**indicative** book of market-maker quotes, the real book is on `orderbooks/rfq/{m}`, post-only orders are rejected and
all placed orders pay the taker fee (docs). The tracker must:

- Fetch `/info/markets` at start-up and every 10 minutes; log `isRfq`, `status`, `active` for tracked markets.
- If a tracked market has `isRfq == true`, log a loud warning and also subscribe `orderbooks/rfq/{m}`.

### 2.9 Binance USD-M futures reference feed

- `wss://fstream.binance.com/ws/{symbol}@bookTicker` (e.g. `hypeusdt`). Fields: `u` update id, `b`/`B` bid px/qty,
  `a`/`A` ask px/qty, `T` transaction time, `E` event time (ms). ~75–160 msg/s on HYPEUSDT.
- Binance futures also runs in AWS Tokyo; measured TCP round trip from the US identical to Extended's (179 ms).
- Binance blocks some regions. From a US IP, futures REST returns HTTP 451 and some streams (`@aggTrade`,
  `@markPrice`, `@forceOrder`, `@kline`) **connect and then deliver nothing** while `@bookTicker` works. From Tokyo this
  should be fine, but **build a watchdog** (§4.5) — a successful subscription is not proof of delivery.
- Binance's `E` is engine time, so `recv_utc − E` has a hard floor at the one-way network delay; this is the tracker's
  **clock sanity check** (§5.4).

---

## 3. The feed design we settled on, and why

### 3.1 Measured from the US (baseline the Tokyo numbers will be compared to)

HYPE-USD, two 15-minute windows, all feeds on separate sockets in one process (same clock). "Delay" = local receive
time (NTP-corrected UTC) − the message's `ts`. Network round trip US↔Tokyo = 179 ms, so ≈ 89 ms one-way.

| Feed | msgs/s | Delay p01 / p50 / p90 (ms) | Touch race vs `depth=1` |
|---|---|---|---|
| v1 `depth=1` | 7–12 | 90 / 100 / 122–124 | reference |
| v1 `depth=1`, second socket | same | same | taking the first of the two improves p90 by 1–3 ms, p99 by 20–28 ms |
| v1 `publicTrades` (vs match `T`) | 0.3–0.5 | 93 / 102 / 131 | trade message arrives **~7–9 ms before** `depth=1` shows the post-trade touch (median) |
| v1 full book | 7–9 | 106 / 134–148 / 188–204 | shows only **46–50 %** of touch states; arrives **+31 to +44 ms** later (median), +92 to +109 ms (p90) |
| v2 `depth=1` (send-time `ts`) | 7 | 89 / 93 / 110–122 | same as v1 (±1 ms) |
| v2 full (send-time `ts`) | 7 | 90 / 95 / 112 | +34 ms vs touch (median) |
| all-markets `depth=1` | ~390 total | 90 / 101 / 134 | p99 +155 ms (head-of-line blocking) |
| all-markets full | ~360 total | 105 / 135 / 207, p99 2 s | p99 +1.7 s |
| mark / index | 0.7 | 87 / 92 / 110 | updates only every ~1.5 s |
| Binance bookTicker | 75–160 | 87 / 93 / 112–121 | — |

Expected in Tokyo (US delay − 89 ms network): touch p50 ≈ 10 ms, full book p50 ≈ 45 ms, trades p50 ≈ 13 ms.
**These are the numbers the tracker exists to replace with real measurements.**

### 3.2 Why not "snapshot once, then update the book from trades"?

Tested and rejected: only **11 %** of touch changes have a trade within 50 ms — the rest are makers placing and
cancelling orders, which never appear on the trade stream. A book maintained from trades alone had the wrong touch
after 10 s and a mid **4–19 bps** wrong after 10–120 s. Use trades only as a *provisional* early update (§3.4).

### 3.3 Why the full book is slow even co-located

The full book's `ts` is the event time, but the venue holds changes for its 100 ms batch: compared with the
`depth=1` feed for the identical touch state, full's `ts` is ≈ −4 ms (same event) while it **arrives +31–44 ms later**.
Co-location removes the 89 ms of network, not this hold. Hence: touch from `depth=1`, depth from the full book.

### 3.4 Production feed set (what the tracker must subscribe to, per market)

| Socket | URL / subscription | Role |
|---|---|---|
| `ext_d1_a` | v1 `orderbooks/{m}?depth=1` | touch source |
| `ext_d1_b` | same URL, separate connection | race/backup — take whichever arrives first |
| `ext_full` | v1 `orderbooks/{m}` | full-depth book (deltas) |
| `ext_trades` | v1 `publicTrades/{m}` | trades; provisional early touch update |
| `ext_rpc` | v2 rpc: `orderbooks` depth `1` + depth `full` + `trades` for `{m}` | same data with **send-time** `ts` → transport latency; also races v1 |
| `ext_rpc_d1` | v2 rpc: `orderbooks` depth `1` only | is a single-topic v2 socket faster than a multiplexed one? |
| `bin_top` | Binance `{sym}@bookTicker` | reference price + clock sanity check |
| `ext_markets` (REST poll, 10 min) | `/api/v1/info/markets` | `isRfq`/status watch |
| *optional controls* | all-markets `?depth=1`, all-markets full, mark, index | rate/tail comparison only; off by default |

Default markets: `HYPE-USD`, `BTC-USD`, `ETH-USD`, `SOL-USD` (Binance: `hypeusdt`, `btcusdt`, `ethusdt`, `solusdt`).
Configurable. **One socket per (feed, market)** — never put a busy market on a quiet market's socket.

### 3.5 The merged book (implement it — it is what we ultimately measure)

Maintain, per market, one in-memory view that a quoter would read:

1. **Full book** from `ext_full` deltas (dict price → qty per side; `c` sets, `"0"` deletes; snapshot replaces).
2. **Touch override:** on every `depth=1` message (either socket, first arrival wins — dedupe by `(ts, bid px, bid
   qty, ask px, ask qty)`) whose `ts` ≥ the full book's last `ts`, overwrite the top level's price and size.
3. **Crossed-level drop (view only):** while the `depth=1` touch is newer than the full book, ignore full-book bids
   priced above the `depth=1` bid and asks below the `depth=1` ask (they are stale). Never mutate the stored book for
   this — the next full delta fixes it.
4. **Provisional trade update:** on a trade, subtract its quantity from the resting opposite side at/through its
   price immediately (aggressor `BUY` eats asks). The next `depth=1` or full message overwrites it.
5. Each field carries `source` and `source_ts` so age is always known.

The tracker records, for every change of the merged touch, **which source delivered it first and how far ahead of
the others it was.**

---

## 4. What to build

### 4.1 Language and libraries

Python 3.12 is fine at millisecond resolution if done carefully (one process, asyncio, `uvloop` on Linux, `orjson`
for parsing, `websockets` ≥ 13). Requirements:

- **Stamp first:** call `time.time_ns()` (UTC wall) **and** `time.perf_counter_ns()` (monotonic) as the very first
  line after `recv()` returns, before parsing.
- Keep parsing light in the receive path. Write raw bytes + stamps to a queue; a writer task does disk I/O.
- Measure **event-loop lag** (schedule a 10 ms timer, record lateness) — if loop lag p99 exceeds a few ms, your
  timestamps are contaminated and the report must say so.
- Optional later: a Rust/C++ receiver with kernel RX timestamps (`SO_TIMESTAMPING`) to separate kernel→user delay.
  Not needed for v1 of the tracker; keep the stamping layer swappable.

### 4.2 Suggested repo layout

```
ext-latency-tracker/
├── README.md                 # quick start (local run, EC2 run, report)
├── SPEC.md                   # this file
├── pyproject.toml            # deps: websockets, orjson, uvloop (linux), pyarrow, numpy, pandas, boto3 (optional)
├── config.yaml               # markets, feeds on/off, output dir, S3 bucket, run duration
├── tracker/
│   ├── main.py               # CLI: `python -m tracker.main --config config.yaml --minutes 60` | `--test` (§4.6)
│   ├── feeds.py              # feed definitions (URLs, subscribe msgs) + parsers per feed
│   ├── conn.py               # one coroutine per socket: connect, subscribe, stamp, reconnect w/ backoff, watchdog
│   ├── book.py               # full-depth book, seq checks, catch-up detection, merged view (§3.5)
│   ├── clock.py              # chrony/PHC status reader, NTP fallback, offset logging
│   ├── netprobe.py           # periodic TCP-connect RTT + WS ping RTT to every host/IP
│   ├── writer.py             # rotating raw archive (zstd JSONL or parquet) + per-minute summary rows
│   └── stats.py              # streaming percentiles (t-digest or HDR histogram) for live summaries
├── analysis/
│   └── report.py             # raw archive -> report.md (+ optional HTML/CSV) with every table in §5
├── deploy/
│   ├── launch.sh             # one-command EC2 launch (AWS CLI)
│   ├── teardown.sh
│   ├── cloud-init.yaml       # user-data: packages, chrony, repo clone, venv, systemd, auto-shutdown
│   ├── chrony.conf
│   ├── ext-tracker.service   # systemd unit
│   └── az_probe.sh           # optional: compare AZs (§6.2)
└── tests/
    ├── test_book.py          # delta application, seq gap -> resync, snapshot replace
    ├── test_catchup.py       # replay detector on a synthetic stream
    └── fixtures/             # a few real messages copied from §2
```

### 4.3 Per-message record (raw archive)

| field | meaning |
|---|---|
| `feed` | e.g. `ext_d1_a`, `ext_full`, `ext_rpc`, `bin_top` |
| `conn_id` | increments on each reconnect of that feed |
| `topic` | v2 subscription id, else empty |
| `recv_wall_ns` | `time.time_ns()` at receipt (system clock, disciplined by chrony) |
| `recv_mono_ns` | `time.perf_counter_ns()` at receipt |
| `venue_ts_ms` | `ts` (Extended), `E` (Binance) |
| `match_T_ms` | trades only: max `T` in the batch |
| `seq` | as sent |
| `seq_gap` | bool — the expected seq was skipped |
| `is_catchup` / `is_backfill` | replay flags (§2.4, §2.7) |
| `raw` | the message bytes, verbatim |

Rotate hourly; compress (zstd). Size estimate: ~30–60 MB/hour for 4 markets with optional controls off; ~1 GB/day.

### 4.4 Per-minute summary record (small, for live monitoring)

Per feed and market: message count, seq gaps, reconnects, delivery-delay p01/p50/p90/p99, transport-delay
percentiles (v2), touch-race lag percentiles, merged-touch age percentiles, loop-lag p99, clock offset and
estimated error (§5.4), RTT min/median per host.

### 4.5 Robustness requirements

- Reconnect with jittered exponential backoff (0.5 s → 30 s). On a `seq` gap, close and reconnect (the full book
  cannot be repaired otherwise).
- **Watchdog:** any feed with zero messages for > 30 s (book feeds) or > 15 min (trades) after warm-up → log
  `SILENT FEED` and reconnect. A quiet trade feed is normal; a quiet book feed is not.
- Never let one socket block another: one task per socket; the writer is decoupled via a bounded queue (drop-and-count
  if it ever fills, and report the count).
- Graceful shutdown on SIGTERM: flush files, write a final summary, upload.

### 4.6 Test mode (15 minutes, then the numbers)

`python -m tracker.main --config config.yaml --test` must:

1. **Check the clock first** (§5.4): wait up to 60 s for chrony to converge; if the error bound is still > 1 ms, keep
   going but print `CLOCK NOT CONVERGED` at the top of the output and in the report.
2. **Capture for 15 minutes** (`--test-minutes N` overrides) with the full production feed set of §3.4 for every
   configured market, plus the optional control feeds (all-markets streams, mark/index) — test mode exists to compare
   them, and 15 min of them is only ~100–150 MB. Network probes (§5.6) every 30 s instead of 60.
3. Use the same warm-up exclusion as the long run (first 15 s after each connect, catch-up/backfill rows), so a
   15-minute result is directly comparable to a 24 h one.
4. **On completion, run the report in-process** on that run's archive and:
   - print a compact summary to stdout (and therefore the systemd journal / console log), one table per market:
     feed × {msgs/s, delay p01/p50/p90/p99, transport p50 (v2), touch-race lag p50/p90 and % of states shown},
     then the full-book batch hold, min RTT per IP, venue clock offset estimates, clock error bound, processing time
     p50/p99, seq gaps / reconnects / silent feeds;
   - write `report.md` and `summary.csv` (same format as §5.9) plus `summary.json` (machine-readable, the same numbers);
   - upload the run folder to S3 if configured;
   - exit 0 if all continuity checks passed (no seq gaps outside reconnects, no silent book feed, clock bound ≤ 1 ms),
     else exit 2 with the failing checks listed — so a script can tell a clean test from a broken one.
5. Mark everything with `mode: test` and the duration, and add one line to the report: *"15-minute sample: p99 and
   max are indicative only; tail estimates need the 24 h run."*

Test mode is also the first thing `launch.sh --test` runs on EC2 (§6.3), and the acceptance gate before a long run.

---

## 5. Metrics and the report

All latency numbers in ms. Report p01, p10, p50, p90, p99, max and n. **Exclude** catch-up/backfill rows, the first
15 s after each connect, and minutes where the clock-error bound (§5.4) exceeds 1 ms.

### 5.1 Delivery delay (per feed)

`delay = recv_wall_ms − venue_ts_ms` (trades: `recv_wall_ms − match_T_ms`). On v1 books this is network + venue
batching/processing + venue clock offset.

### 5.2 Transport delay (v2 only)

`transport = recv_wall_ms − v2 ts` (send time). Its minimum ≈ one-way network + venue clock offset. Co-located we
expect sub-millisecond network, so the **venue clock offset can dominate** — see §5.4 for how to bound it.

### 5.3 Batch hold of the full book

For each full-book book state, find the same state on the v2 full feed: `hold = v2_ts − v1_ts`. Report the
distribution (US measurement: ≈ 45 ms median). This is server-side and independent of where we sit.

### 5.4 Clock quality and venue clock offset — do not skip

- On EC2, chrony disciplined by the **Amazon Time Sync Service** (`169.254.169.123`, link-local, available on every
  instance) normally keeps the system clock within well under a millisecond of UTC; on instance types that expose the
  **ENA PTP hardware clock** (`/dev/ptp*` after enabling the ENA PHC; Nitro-based types such as c7i) it is
  microsecond-level. Log `chronyc tracking` every minute (offset, root dispersion, root delay); compute
  `clock_error_bound = |offset| + root_dispersion + root_delay/2` and store it.
- **Negative latencies mean a clock problem, not a fast feed.** If any feed's p01 delay is < 0, the venue's clock is
  ahead of ours (or ours is wrong). Report it, don't hide it.
- Estimate the venue's clock offset: `venue_offset ≈ p01(v2 transport) − min_RTT/2`, using the RTT from §5.6.
  Do the same for Binance with `E`. Report both.

### 5.5 Touch race (the most important table)

For every distinct merged-touch state (bid px, bid qty, ask px, ask qty), record the first receipt time on every
source that ever shows it: `ext_d1_a`, `ext_d1_b`, `ext_rpc` (depth 1), `ext_rpc_d1`, `ext_full`, and "provisional
from trade". Report for each source: share of states it shows, lag vs the earliest source (p50/p90/p99), and how often
it was the earliest. Also report the gain from racing two `depth=1` sockets (`min(a, b)` vs `a` alone).

### 5.6 Network probes

Every 60 s, for each resolved IP of `api.starknet.extended.exchange` and `fstream.binance.com`: TCP connect time
(SYN→established) ×5, plus WebSocket ping→pong RTT on each live socket. Report min/median per IP. Extended resolved
to three AWS Tokyo IPs (`54.95.235.34`, `13.193.121.181`, `35.76.199.77` on 2026-10-28 — these may change; resolve
fresh). Co-located, one of them should be ≪ 1 ms.

### 5.7 Internal processing time

`recv → merged book updated` per message (perf_counter), and event-loop lag. Report p50/p99. This tells us whether
Python is fast enough for production or whether the receiver needs to move to a compiled language.

### 5.8 Throughput and continuity

Messages/s per feed, bytes/s, seq gaps, reconnects, silent-feed events, writer drops — per hour.

### 5.9 Report format

`python -m analysis.report --data <dir> --out report.md` producing one Markdown file with: run metadata (instance
type, AZ ID, kernel, Python version, chrony state, duration, markets), then §5.1–§5.8 tables, then a side-by-side of
**Tokyo vs the US baseline in §3.1**. Also emit `summary.csv`. Make the report reproducible from the raw archive alone.

### 5.10 Acceptance criteria

**Test mode (gate before any long run):**

- [ ] `--test` finishes in 15 min + < 2 min of report time, prints the summary, writes `report.md`/`summary.csv`/
      `summary.json`, exits 0 on a healthy run.
- [ ] Every feed in §3.4 delivered messages; 0 seq gaps outside reconnects; ≥ 1 exact full-book replay check passed.
- [ ] Exit code 2 when a feed is deliberately muted (e.g. point one URL at a non-existent market).

**Long run:**

- [ ] Runs 24 h unattended on EC2 with 0 crashes; reconnects handled; watchdog fires on a deliberately muted feed.
- [ ] Full-book replay is exact: at every mid-stream venue SNAPSHOT, the locally maintained book equals the snapshot
      (all levels) — report the count of exact matches (expected 100 %, excluding windows spanning a reconnect).
- [ ] `seq` gaps reported (expected 0 in steady state).
- [ ] Catch-up rows detected after every connect and excluded.
- [ ] Clock error bound < 1 ms for > 99 % of minutes; no negative p01 delays left unexplained.
- [ ] Report contains every table in §5 for at least `HYPE-USD` and `BTC-USD`, over ≥ 6 h including a busy US-session
      window (13:00–21:00 UTC) and a quiet Asia window.

---

## 6. Where to run it

### 6.1 Placement

| Choice | Recommendation | Why |
|---|---|---|
| Region | `ap-northeast-1` (Tokyo) | Extended's engine is there (docs); Binance futures too. |
| Availability zone | the AZ whose **ID** is `apne1-az4` | Extended's docs name `ap-northeast-1a`, **AZ ID `apne1-az4`**. AZ *names* (`1a`, `1c`, `1d`) map to different physical zones in different AWS accounts — **always select by AZ ID**. |
| Instance type | `c7i.large` (2 vCPU, 4 GiB), on-demand | Non-burstable, Nitro, ENA enhanced networking, good single-thread speed. Avoid `t3`/`t4g`: CPU credit throttling causes timing jitter. `c7g.large` (Graviton) also fine (uvloop and orjson build on arm64). Network-optimized `c7gn`/`c6in` are unnecessary at these message rates. |
| Purchase | on-demand; spot is OK for experiments | A spot interruption just ends the run; results are in S3. |
| Networking | default VPC, **public subnet in apne1-az4 with a public IPv4** | Needs outbound internet. **Do not route through a NAT gateway** — it adds a hop and latency. (A public IPv4 costs ~$0.005/h.) No inbound ports needed. |
| Access | **SSM Session Manager**, no SSH key, security group with **no inbound rules** | Simpler and safer. Needs an instance role with `AmazonSSMManagedInstanceCore`. |
| Storage | 30 GB gp3 root | Raw archive is ~1 GB/day; sync to S3 hourly and prune local files older than 2 days. |
| OS | Amazon Linux 2023 or Ubuntu 24.04 | Both ship chrony pointed at the Amazon Time Sync Service by default; verify. |
| Cost | roughly $0.1–0.15/h for `c7i.large` in Tokyo (check current pricing) | ≈ $3/day. The auto-shutdown timer (§6.3) prevents a forgotten instance. |

### 6.2 Verify the AZ empirically (optional but cheap)

The docs' AZ statement is the starting point, not proof. `deploy/az_probe.sh` should launch three tiny instances
(one per AZ ID: `apne1-az1`, `apne1-az2`, `apne1-az4`), run `netprobe` + a 10-minute `depth=1` capture on each, print
min TCP connect time per Extended IP and the v2 transport p01, then terminate them. Use the AZ with the lowest numbers
(expected: `apne1-az4`, with one Extended IP at ~0.1–0.5 ms). Also note which Extended IP is closest — pinning the
connection to that IP (with the correct SNI/Host header) may be worth testing.

Find the AZ name for an AZ ID in your account:

```bash
aws ec2 describe-availability-zones --region ap-northeast-1 \
  --query "AvailabilityZones[].{name:ZoneName,id:ZoneId}" --output table
```

Find a default-VPC subnet in a given AZ ID:

```bash
aws ec2 describe-subnets --region ap-northeast-1 \
  --filters Name=availability-zone-id,Values=apne1-az4 Name=default-for-az,Values=true \
  --query "Subnets[0].SubnetId" --output text
```

### 6.3 One-command deployment (`deploy/launch.sh`)

Prerequisites on the operator's machine: AWS CLI v2 configured with an account that can create EC2, IAM and S3
resources. The script should be idempotent and do:

1. Create (if missing) an S3 bucket for results and an IAM role + instance profile `ext-tracker-role` with
   `AmazonSSMManagedInstanceCore` and `s3:PutObject`/`s3:ListBucket` on that bucket only.
2. Create (if missing) a security group `ext-tracker-sg` with **no inbound rules** (default all-outbound).
3. Resolve the subnet for AZ ID `apne1-az4` (above) and the latest AMI via SSM parameter
   `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64` (or the arm64 variant for Graviton).
4. `aws ec2 run-instances` with: instance type, subnet, `--associate-public-ip-address`, the instance profile, the
   security group, `--instance-initiated-shutdown-behavior terminate`, tags (`Project=ext-latency-tracker`), and
   `--user-data file://deploy/cloud-init.yaml` with the repo URL, branch, bucket, markets, `TRACKER_ARGS` and
   `RUN_MINUTES` substituted.
5. Print the instance id and the commands to connect (`aws ssm start-session --target <id>`), tail logs
   (`journalctl -u ext-tracker -f`), and fetch results (`aws s3 sync s3://<bucket>/<run-id> ./results`).

Modes:

| Command | `TRACKER_ARGS` | `RUN_MINUTES` (shutdown budget) | What you get |
|---|---|---|---|
| `deploy/launch.sh --test` | `--test` | 30 (15 capture + boot, clock convergence, report, upload) | the latency tables ~25 min after launch; instance self-terminates |
| `deploy/launch.sh --hours 24` | `--minutes 1440` | 1440 + 30 | the full measurement run |

With `--test`, `launch.sh` should also **wait for the result**: poll S3 (every 30 s, up to 40 min) for
`<run-id>/summary.json`, then download the run folder and print the summary table locally, so one command goes from
nothing to Tokyo latency numbers on the operator's screen. If nothing arrives, print the last 100 lines of the
console output (`aws ec2 get-console-output`) to show what went wrong.

`deploy/teardown.sh` terminates every instance tagged `Project=ext-latency-tracker` (and optionally the role, SG and
bucket).

If the operator prefers infrastructure-as-code, the same resources fit in ~60 lines of Terraform or one CloudFormation
template; the shell script is recommended for fewer dependencies.

### 6.4 `deploy/cloud-init.yaml` (what the instance does on first boot)

```yaml
#cloud-config
package_update: true
packages: [git, python3.12, python3.12-pip, chrony, zstd]   # AL2023 names; adjust for Ubuntu
write_files:
  - path: /etc/chrony.d/aws.conf
    content: |
      server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4
      # if /dev/ptp0 exists (ENA PHC enabled): refclock PHC /dev/ptp0 poll 0 delay 0.000010 prefer
  - path: /etc/systemd/system/ext-tracker.service
    content: |
      [Unit]
      Description=Extended feed latency tracker
      After=network-online.target chronyd.service
      Wants=network-online.target
      [Service]
      User=ec2-user
      WorkingDirectory=/opt/ext-latency-tracker
      ExecStart=/opt/ext-latency-tracker/.venv/bin/python -m tracker.main --config config.yaml ${TRACKER_ARGS}
      # test mode exits on its own when done; don't restart a finished test (exit 0 or 2)
      Restart=on-failure
      SuccessExitStatus=2
      RestartSec=5
      Nice=-5
      [Install]
      WantedBy=multi-user.target
runcmd:
  - systemctl enable --now chronyd && systemctl restart chronyd
  - git clone --depth 1 -b ${BRANCH} ${REPO_URL} /opt/ext-latency-tracker
  - chown -R ec2-user /opt/ext-latency-tracker
  - sudo -u ec2-user python3.12 -m venv /opt/ext-latency-tracker/.venv
  - sudo -u ec2-user /opt/ext-latency-tracker/.venv/bin/pip install -e /opt/ext-latency-tracker
  - sed -i "s|^s3_bucket:.*|s3_bucket: ${BUCKET}|" /opt/ext-latency-tracker/config.yaml
  - systemctl daemon-reload && systemctl enable --now ext-tracker
  # hourly S3 sync of finished files
  - echo "15 * * * * ec2-user aws s3 sync /opt/ext-latency-tracker/data s3://${BUCKET}/$(hostname)/ --exclude '*.partial'" > /etc/cron.d/ext-sync
  # safety: power off (= terminate, see launch flags) after the run budget
  - shutdown -h +${RUN_MINUTES}
  # test mode: power off as soon as the tracker exits and its results are uploaded (no waiting for the budget)
  - if echo "${TRACKER_ARGS}" | grep -q -- --test; then (while systemctl is-active --quiet ext-tracker; do sleep 10; done; aws s3 sync /opt/ext-latency-tracker/data s3://${BUCKET}/$(hostname)/; shutdown -h now) & fi
```

Notes for the implementer:

- `${REPO_URL}`, `${BRANCH}`, `${BUCKET}`, `${TRACKER_ARGS}`, `${RUN_MINUTES}` are **template placeholders** that
  `launch.sh` fills in (e.g. with `envsubst`) before passing the file as user-data — not variables on the instance.

- If the repo is private, use a GitHub deploy key or fine-grained token stored in SSM Parameter Store
  (`SecureString`), fetched in `runcmd`. Never put a token in user-data in plain text.
- Wait for chrony to converge (`chronyc waitsync 30 0.001`) before starting the tracker, and have the tracker refuse to
  start if the offset is > 1 ms.
- Run the final report on the instance before shutdown (`ExecStopPost` or a timer) and upload `report.md` too.
- Optional: a Dockerfile (`--network host`) for local runs; on EC2 the plain venv + systemd route has fewer moving parts.

### 6.5 Local development

The tracker must also run on a laptop (`python -m tracker.main --minutes 5`) so it can be tested before deploying.
From non-Tokyo locations the numbers will include ~tens to hundreds of ms of network; that is expected. On machines
without chrony, fall back to SNTP against `time.google.com` / `time.cloudflare.com` (best-of-5, keep the lowest-RTT
sample; offset error ≤ RTT/2) and record that bound.

---

## 7. Gotchas checklist (all hit in practice)

1. `User-Agent` missing → rejected.
2. Trade stream path is `/publicTrades/{m}`, not `/trades/{m}`.
3. `depth` accepts only `1`.
4. REST orderbook is tens of seconds stale — never use it as truth or to seed.
5. Book stream replays on connect with original timestamps and no flag; trades send a backfill batch.
6. Apply delta `c` (resulting qty), not `q` (signed change).
7. v2 RPC: `seq` is per connection across topics; `ts` is send time, not event time.
8. Negative latency = clock offset problem; correct and bound the clock first.
9. A subscription that succeeds may still deliver nothing (watchdog).
10. AZ names differ per account — select by AZ ID `apne1-az4`.
11. When joining trades to books, match each trade to the book state strictly **before** it (`<`), not `<=`: the
    same-millisecond book update already shows the post-trade book.
12. Don't run heavy analysis inside the capture process — it stalls the event loop and corrupts timestamps.

---

## 8. Milestones

1. **Local skeleton:** connect to every feed in §3.4 for one market, stamp and archive raw messages, seq + catch-up
   handling, watchdog. Unit tests for the book (§4.2 tests).
2. **Merged book + metrics:** §3.5 and §5.1–5.8 computed offline by `analysis/report.py` from the archive; live
   per-minute summaries.
3. **Test mode + deploy:** `--test` (§4.6) locally first, then `launch.sh --test` end to end on EC2 (launch → 15-min
   capture → summary printed on the operator's machine → instance terminated). Deliver that first test report.
4. **AZ check:** `az_probe.sh` comparing `apne1-az1/az2/az4` (optional).
5. **Measurement run:** ≥ 24 h on HYPE/BTC/ETH/SOL in `apne1-az4`; deliver `report.md` with the Tokyo-vs-US table.

### Deliverables

The repo; a `report.md` from the 24 h run; and a short summary answering: what is the co-located delay (p50/p90/p99)
of (a) the touch via `depth=1`, (b) the full book, (c) trades, (d) Binance bookTicker; how much the second `depth=1`
socket and the v2 socket buy; the full-book batch hold; the venue clock offsets; and whether Python's internal
processing time is negligible next to these.
