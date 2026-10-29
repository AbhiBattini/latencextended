# Spec: per-socket TCP state logging and stall attribution

Addendum to [SPEC.md](SPEC.md). It adds the measurement needed to say *who* caused a delivery stall: our host,
the network path, or Extended. Target: in place before the 24 h run.

## 1. Why

The first EC2 test (`test-ec2`, 2026-10-29, c7i.large, `apne1-az4`, 15.4 min) had 59 stall episodes in which at
least one socket delivered data more than 250 ms late. The worst was 2.8 s. What the existing data already shows:

- **Not our event loop.** Per-minute loop-lag max ≤ 31 ms; p99 ≈ 1 ms.
- **Not the host as a whole.** Binance `bookTicker`, on the same process and NIC at ~1,200 msg/s, was on time in
  57 of 59 episodes.
- **Per connection, not per server.** In the worst episode, three sockets on `13.193.121.181` froze for ~2.7 s while
  a fourth socket on the same IP (`ext_d1_a:ETH-USD`) maxed out at 25 ms.
- **Hold-then-dump.** The frozen socket received nothing for 2.7 s, then ~3 s of messages within the same
  millisecond, with venue `ts` evenly spread across the gap.
- **One server is worse.** Share of rows >250 ms late: `13.193.121.181` 1.58 %, `54.95.235.34` 0.61 %,
  `35.76.199.77` 0.36 %, the all-markets `depth=1` stream 0.008 %.

Hold-then-dump has three possible causes, and today's data cannot tell them apart:

| Cause | Where the bytes were during the gap |
|---|---|
| **OURS**: we did not read | in our kernel receive queue |
| **LOSS**: a lost segment, then retransmit back-off (RTO 200 → 400 → 800 ms…) | later segments in our out-of-order queue, waiting for the retransmit |
| **VENUE**: Extended held the data (send queue, publisher, load balancer) | not on the wire at all |

## 2. What to measure

### 2.1 Per-socket TCP state (Linux only)

For every live WebSocket, read the following from the underlying TCP socket:

| Field | Source | Tells us |
|---|---|---|
| `inq` | `ioctl(fd, SIOCINQ)` (`FIONREAD`) | bytes the kernel has received that the app has not yet read. **>0 during a gap ⇒ OURS** |
| `bytes_received` | `TCP_INFO.tcpi_bytes_received` (u64) | whether bytes arrived during the gap. **Flat ⇒ nothing on the wire** |
| `segs_in`, `data_segs_in` | `TCP_INFO` | same, at segment granularity |
| `rcv_ooopack` | `TCP_INFO.tcpi_rcv_ooopack` (kernel ≥ 5.4) | out-of-order packets received. **Increment ⇒ LOSS on the inbound path** |
| `dsack_dups` | `TCP_INFO.tcpi_dsack_dups` | duplicate segments (spurious retransmits) |
| `last_data_recv` | `TCP_INFO.tcpi_last_data_recv` (ms since last data) | an independent gap clock in the kernel |
| `rtt`, `rttvar`, `min_rtt` | `TCP_INFO` (µs) | path RTT; our side is mostly ACKs, so a rough figure only |
| `rcv_space`, `rcv_ssthresh` | `TCP_INFO` | receive-window context; a collapsing window points at OURS |
| `state`, `ca_state` | `TCP_INFO` | sanity |

Unpack `TCP_INFO` with a `struct` layout for the running kernel (AL2023 = 6.1). Read `getsockopt(..., 256)`
and take only the fields whose offset fits in the returned length, so an older kernel degrades instead of
crashing. Our own send-side counters (`tcpi_retrans`, `tcpi_total_retrans`) are logged too, but they only count
*our* retransmits (pings, subscribes). Extended's retransmits to us are invisible from our side, so loss must be
inferred from `rcv_ooopack` and DSACK.

### 2.2 System-wide TCP counters

Once per second, take the deltas from `/proc/net/netstat` (`TcpExt`) and `/proc/net/snmp` (`Tcp`):
`TCPOFOQueue`, `TCPOFODrop`, `TCPDSACKOldSent`, `TCPDSACKOfoSent`, `TCPToZeroWindowAdv`, `TCPFromZeroWindowAdv`,
`TCPRcvQDrop`, `TCPBacklogDrop`, `InSegs`, `RetransSegs`, `InErrs`. `TCPToZeroWindowAdv` > 0 means we told a peer
to stop sending: OURS.

Also once per second: `/proc/net/softnet_stat` (dropped and time-squeezed per CPU), and the NIC counters from
`ethtool -S eth0` if available. The ENA `bw_in_allowance_exceeded`, `pps_allowance_exceeded` and
`conntrack_allowance_exceeded` counters show an instance-level network cap: OURS, at the instance.

## 3. Sampling and storage

Bounded volume over 24 h, full resolution only where it matters:

1. **Ring buffer, 100 ms.** Sample every socket at 10 Hz into a per-socket in-memory ring that holds the last
   10 s. Nothing is written in the normal case. 40 sockets × 10 Hz is about 400 cheap syscalls/s.
2. **Stall trigger.** A socket is *stalled* when the receive gap since its previous message is > 250 ms, or a
   message arrives > 250 ms late against venue `ts`. Binance is judged on the gap only, since its `E` is
   comparable but noisier. On a trigger, write the ring for **that socket and every socket on the same peer IP**,
   keep sampling them at 10 Hz for 3 s after the gap closes, then write that too. Include a snapshot of the
   system counters (§2.2) and the current loop-lag value.
3. **1 Hz summary.** Every second, one record per socket with the counter deltas, into its own file.

Files go in the run folder and follow the existing writer conventions (zstd, hourly rotation, frame closed every
30 s):

| File | Contents |
|---|---|
| `tcp/tcp-1s-YYYYmmddTHH-*.jsonl.zst` | `{wall_ns, feed, conn_id, peer, inq, d_bytes_received, d_segs_in, d_rcv_ooopack, d_dsack_dups, last_data_recv_ms, rtt_us, rttvar_us, rcv_space}` |
| `tcp/tcp-stall-*.jsonl.zst` | one record per stall: `{stall_id, feed, conn_id, peer, gap_start_ns, gap_end_ns, max_delay_ms, samples: [...100 ms ring before and after...], sys_deltas: {...}, loop_lag_ms}` |
| `meta.jsonl` | one `{"type":"tcp_stall", stall_id, feed, peer, gap_ms, verdict}` line per stall, for the report |

The sampler must never block the receive path. Run it as its own asyncio task, using the existing `fd`, and
never call `recv` from it.

## 4. Attribution rule

Evaluate each stall over the window `[gap_start, gap_end]` on the stalled socket. The first rule that matches
gives the verdict:

1. **OURS**: `max(inq) > 0` throughout the gap, or `TCPToZeroWindowAdv` increased, or loop lag ≥ gap/2, or the
   ENA allowance counters increased.
2. **LOSS**: `rcv_ooopack` or system `TCPOFOQueue` increased during the gap, and `bytes_received` kept growing
   (the later segments arrived out of order) or jumped at the end, after a retransmit.
3. **VENUE**: `bytes_received` flat across the gap (within one keep-alive frame), `inq` = 0 and no out-of-order
   packets.
4. **UNKNOWN**: anything else. Report it and do not force it into a class.

A stall that hits several sockets at once is classified per socket, then summarised: e.g. "3 sockets, all VENUE,
all on 13.193.121.181".

## 5. Code changes

| File | Change |
|---|---|
| `tracker/tcpinfo.py` (new) | `read_tcp_info(sock) -> dict`, `read_inq(sock) -> int`, `SysCounters.delta() -> dict`, layout table per kernel, no-op stubs off Linux |
| `tracker/conn.py` | in `_on_connect`, keep the raw socket: `ws.transport.get_extra_info("socket")`. In `_recv_loop`, keep the previous receive time and flag a gap > `tcp.stall_gap_ms` to the sampler (a counter bump only, no syscall on the hot path) |
| `tracker/main.py` | new task `_tcp_sampler` (10 Hz ring, 1 Hz summary, stall dumps); new task `_sys_counters` (1 Hz) |
| `tracker/writer.py` | a second output stream for `tcp/` files, using the same rotation and frame logic |
| `tracker/config.py`, `config.yaml` | `tcp: {enabled: true, sample_ms: 100, ring_s: 10, post_s: 3, stall_gap_ms: 250, summary_s: 1}` |
| `analysis/report.py` | new **§5.9 Stall attribution**: counts by verdict, by peer IP and by feed; the 10 worst stalls with their verdict and the evidence (inq, Δbytes, Δooo); share of stall-seconds by verdict |
| `deploy/cloud-init.yaml` | install `ethtool`; optionally `tcpdump` (see §7) |

## 6. Tests

- **Unit:** pack a known `TCP_INFO` byte string for kernel 6.1 and check the parsed fields; a truncated buffer
  parses without raising; off Linux the reader returns `{}`.
- **End-to-end against `tests/mock_venue.py`**, adding two fault modes:
  - `--hold-ms 2000` on one connection: the server stops writing, then flushes. Expected verdict **VENUE**
    (`bytes_received` flat, `inq` = 0).
  - Client-side stall: a test hook blocks the event loop for 2 s with `time.sleep`. Expected verdict **OURS**
    (`inq` > 0 when the loop resumes; loop lag ≈ 2 s).
- **Optional, root only:** `tc qdisc add dev lo root netem loss 5%` against the mock venue. Expected: some stalls
  classified **LOSS**, with `rcv_ooopack` > 0. Mark the test skipped without `CAP_NET_ADMIN`.
- **Overhead:** at the live feed rates, the sampler adds < 1 % CPU, and receive-path `proc_us` p99 does not change
  by more than 5 µs. Measure it in the next `--test` run.

## 7. Optional: header-only packet capture

For a definitive answer on any one stall: a rotating `tcpdump -i eth0 -s 128 -w ... -C 100 -W 20` limited to the
Extended server IPs (host filter built from the resolved IPs at startup). Headers only, no payload decryption needed.
Retransmitted sequence numbers and the arrival time of every segment are then visible. Keep only the files that
overlap a stall (a stall record names its time range); delete the rest each hour. It needs `CAP_NET_RAW`, so run it as
a separate systemd unit, not inside the tracker. Off by default (`tcp.pcap: false`).

## 8. Acceptance for the 24 h run

- Every stall > 250 ms gets a verdict; UNKNOWN is < 10 % of stalls.
- The report's §5.9 answers, per Extended server IP: how many stall-seconds were VENUE, LOSS and OURS.
- Rerunning `--test` reproduces the qualitative result of `test-ec2` (the worst sockets are on one IP), or it
  documents why not.
