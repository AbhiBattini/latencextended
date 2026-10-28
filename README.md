# Extended feed latency tracker

Subscribes to Extended's public market-data feeds the way a low-latency market maker would (plus Binance USD-M
`bookTicker` as a reference), measures how late each piece of information arrives, and runs co-located in AWS Tokyo
(`ap-northeast-1`, AZ ID `apne1-az4`). Read-only: no API key, no account. The full design is in [SPEC.md](SPEC.md).

## Quick start

```bash
python3.12 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
```

Local run (numbers include your network distance to Tokyo):

```bash
python -m tracker.main --config config.yaml --minutes 5
```

Test mode: 15-minute capture of every feed, then the latency tables, `report.md`, `summary.csv`, `summary.json`;
exits 0 when the continuity checks pass, 2 otherwise:

```bash
python -m tracker.main --config config.yaml --test
```

In Tokyo, start with test mode. One command launches the instance, waits for the result, prints the tables here, and
the instance terminates itself:

```bash
deploy/launch.sh --test
```

Once the test looks sane, run the 24 h measurement:

```bash
deploy/launch.sh --hours 24
```

Rebuild a report from a run folder (local or downloaded from S3):

```bash
python -m analysis.report --data results/<run-id> --out report.md
```

Tests (unit tests, plus end-to-end runs against a local mock venue, about 1 min):

```bash
python -m pytest
```

## What gets measured

| Section | Metric |
|---|---|
| 5.1 | Delivery delay per feed: `recv − ts` (trades: `recv − match T`), p01…p99/max/n, and the trades' venue publish lag `ts − T` |
| 5.2 | Transport delay on v2 (its `ts` is the send time) |
| 5.3 | Full-book batch hold: the same top-25 book state on v1 full vs v2 full, `v2 ts − v1 ts` plus the arrival difference |
| 5.4 | Clock quality (chrony bound = \|offset\| + root dispersion + root delay/2), venue clock offsets `p01 − minRTT/2` for Extended and Binance |
| 5.5 | Touch race: for every distinct touch state, the first receipt on each source (`ext_d1_a`, `ext_d1_b`, `ext_rpc.d1`, `ext_rpc_d1`, `ext_full`, `ext_rpc.full`, `trade_prov`, `ctl_all_d1`): share shown, % earliest, lag p50/p90/p99; the gain from racing two `depth=1` sockets; merged-touch age |
| 5.6 | TCP connect time to every resolved IP, WS ping→pong and v2 RPC ping per socket, and which IP each socket landed on |
| 5.7 | `recv → merged book updated` per message, event-loop lag |
| 5.8 | Hourly msgs/s, KB/s, seq gaps, connects, silent feeds; writer drops; catch-up/backfill rows; exact full-book replay checks |
| — | Side-by-side with the US baseline of SPEC §3.1 |

Exclusions, as SPEC §5 requires: catch-up rows (connect replay), trade backfill, the first `warmup_s` (15 s) after
every connect, and minutes whose clock error bound is above `clock.max_error_ms` (1 ms).

## Layout

```
tracker/        capture process
  main.py       CLI, Tracker (tasks: sockets, watchdog, loop lag, per-minute summary, net probes, clock, markets poll)
  feeds.py      feed set (SPEC §3.4), URLs, v2 subscribe messages, light header parse
  conn.py       one coroutine per socket: stamp first, seq/gap, catch-up, reconnect + backoff
  book.py       OrderBook, CatchupDetector, MergedBook (§3.5), TouchRace (§5.5)
  engine.py     per-message processing shared by the live path and the report
  clock.py      chrony reader, SNTP fallback
  netprobe.py   TCP connect probes (`python -m tracker.netprobe` standalone)
  writer.py     threaded zstd JSONL archive, hourly rotation; meta.jsonl
  stats.py      mergeable log-linear histogram (bounded memory over 24 h), exact per-minute percentiles
  live.py       per-minute summary record
  upload.py     S3 upload (aws CLI or boto3)
analysis/report.py   archive -> report.md / summary.csv / summary.json / console summary
deploy/         launch.sh, teardown.sh, az_probe.sh, cloud-init.yaml, chrony.conf, ext-tracker.service
tests/          unit tests, fixtures from SPEC §2, mock_venue.py, end-to-end tests
```

### Run folder (`data/<run-id>/`, mirrored to `s3://<bucket>/<run-id>/`)

| File | Contents |
|---|---|
| `raw/raw-YYYYmmddTHH-<start>.jsonl.zst` | One record per message (SPEC §4.3 fields plus `warm`, `proc_us`), venue message embedded verbatim in `raw`. Rotated hourly; a zstd frame is closed every 30 s, so a crashed run's `.partial` file can still be read |
| `meta.jsonl` | run metadata (instance type, AZ ID, kernel, Python), connects/disconnects with peer IP, seq gaps, silent feeds, clock samples, net probes, WS/RPC pings, loop lag, `/info/markets` (isRfq) polls |
| `minutes.jsonl` | live per-minute summaries (SPEC §4.4) |
| `report.md`, `summary.csv`, `summary.json`, `summary.txt` | the report; `summary.txt` is what gets printed to stdout |
| `tracker.log` | the process log (also on stdout / journal) |

## Configuration

See [config.yaml](config.yaml). The keys you're most likely to change: `markets` (Extended market → Binance symbol),
`feeds` (turn production feeds on or off), `controls` (all-markets, mark and index feeds; always on in `--test`),
`url_overrides` (point one feed somewhere else, e.g. at `NOPE-USD` to check that the watchdog fires and the run exits 2),
`s3_bucket`, and `clock.mode` (`auto`, `chrony`, `sntp` or `none`).

CLI overrides: `--markets HYPE-USD,BTC-USD`, `--only-feeds ext_d1_a,ext_rpc_d1`, `--test-minutes N`, `--run-id`,
`--data-dir`, `--s3-bucket`, `--require-clock` (exit 3 if the clock bound is still over the maximum after 60 s), `--no-report`.

## Deployment notes

`deploy/launch.sh` is idempotent. It creates (if missing) the results bucket, IAM role and instance profile
`ext-tracker-role` (SSM core + `s3:PutObject`/`s3:ListBucket` on that bucket only), and security group
`ext-tracker-sg` (no inbound rules). It then resolves the default subnet **by AZ ID** (creating one if the default
VPC has none in that AZ) and the latest AL2023 AMI, and launches with a public IPv4 (no NAT), IMDSv2,
shutdown = terminate, 30 GB gp3 and the tag `Project=ext-latency-tracker`. Access is via SSM:
`aws ssm start-session --target <id>`. For a private repo, store a token as an SSM SecureString and pass
`--git-token-param /path`. Clean up with `deploy/teardown.sh` (`--all` also removes the role and SG).

On the instance, cloud-init clones the repo, installs `deploy/chrony.conf` (Amazon Time Sync Service, plus the
ENA PTP clock when `/dev/ptp0` exists), waits for chrony to converge (`chronyc waitsync`), starts `ext-tracker.service`
and an hourly `ext-sync.timer`, and arms `shutdown -h +RUN_MINUTES` as a safety net. A watcher powers the instance
off as soon as the tracker exits, after a final S3 sync and a copy of the journal.

The tracker itself handles SIGTERM: it flushes, writes the final summary, builds the report and uploads it.

## Choices made where the spec left room

- **AL2023 has no cron**, so the hourly S3 sync is a systemd timer. It also prunes raw files older than 2 days.
- **S3 prefix is the run id** (not the hostname), so `launch.sh` knows exactly which `summary.json` to poll for.
- **Long runs also self-terminate** once the tracker finishes. The safety budget is `minutes + 45` (the spec
  suggests +30) because the end-of-run report replays the whole 24 h archive.
- **Merged book:** the two v1 `depth=1` sockets drive the touch override, as §3.5 specifies. v2 sources take part in
  the touch race but don't feed the merge, because their `ts` is send time and so not comparable with v1 event time.
- **Catch-up detector:** it tracks the running maximum of `excess`. Only growth of more than 5 ms restarts the 1.5 s
  flat window, so normal network jitter on live data can't keep a connection flagged forever. There is also a 60 s
  cap. Separately, rows from the first 15 s after every connect are always excluded.
- **Seq gaps:** these force a reconnect only on sockets that carry a full book (`ext_full`, `ext_rpc`, `ext_rfq`). On
  `depth=1`, trade and Binance sockets they are recorded and counted, since every `depth=1` message is a snapshot and
  nothing needs repairing.
- **Batch hold:** a book state is identified by a hash of its top 25 levels per side.
- **Without chrony** (on a laptop), SNTP best-of-5 gives an offset that the report applies as a correction, with a
  bound of RTT/2. If every minute exceeds the 1 ms bound, minute exclusion is switched off with a loud warning,
  rather than leaving the report empty.

## Status

All of this is verified offline. Unit tests cover the book, replay detection, the merged view, the race and the
histograms. End-to-end tests run `--test` against `tests/mock_venue.py`, which speaks the v1, v2, Binance and REST
protocols of SPEC §2: a healthy run exits 0 with every table filled, a muted feed exits 2, and the report rebuilds
from the archive. It has **not yet been run against the live Extended and Binance endpoints or on EC2**. The first
`deploy/launch.sh --test` is that check (SPEC §8 milestone 3).
