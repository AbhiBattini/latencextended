"""Clock quality (SPEC §5.4, §6.5).

On EC2 chrony is disciplined by the Amazon Time Sync Service (or the ENA PTP hardware clock); we read
`chronyc -c tracking` and compute clock_error_bound = |offset| + root_dispersion + root_delay/2.
Without chrony we fall back to SNTP (best of 5 by lowest RTT); the local clock is not adjusted, so the sample's
offset is recorded as a correction the report applies, and the bound is RTT/2.
"""

from __future__ import annotations

import asyncio
import glob
import logging
import shutil
import socket
import struct
import subprocess
import time

log = logging.getLogger("clock")

NTP_DELTA = 2208988800


def phc_devices() -> list[str]:
    return sorted(glob.glob("/dev/ptp*"))


def parse_chrony_csv(line: str) -> dict | None:
    f = line.strip().split(",")
    if len(f) < 14:
        return None
    try:
        offset = float(f[4]) * 1e3
        root_delay = float(f[10]) * 1e3
        root_disp = float(f[11]) * 1e3
    except ValueError:
        return None
    return {
        "source": "chrony",
        "refid": f[0],
        "ref_name": f[1],
        "stratum": int(f[2]) if f[2].isdigit() else None,
        "offset_ms": offset,
        "last_offset_ms": float(f[5]) * 1e3,
        "rms_offset_ms": float(f[6]) * 1e3,
        "root_delay_ms": root_delay,
        "root_dispersion_ms": root_disp,
        "leap": f[13],
        "bound_ms": abs(offset) + root_disp + root_delay / 2,
        "correction_ms": 0.0,
    }


def chrony_sample() -> dict | None:
    if not shutil.which("chronyc"):
        return None
    try:
        out = subprocess.run(["chronyc", "-c", "tracking"], capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    s = parse_chrony_csv(out.stdout)
    if s and (s["stratum"] in (None, 0) or s["leap"].lower().startswith("not")):
        s["unsynchronised"] = True
        s["bound_ms"] = float("inf")
    return s


def sntp_query(server: str, timeout: float = 1.0) -> tuple[float, float] | None:
    """(offset_ms, rtt_ms); offset = true time - local clock."""
    try:
        addr = socket.getaddrinfo(server, 123, socket.AF_INET, socket.SOCK_DGRAM)[0][4]
    except OSError:
        return None
    pkt = bytearray(48)
    pkt[0] = 0x23  # LI 0, VN 4, mode 3
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        t1 = time.time()
        sec = int(t1) + NTP_DELTA
        frac = int((t1 % 1) * (1 << 32))
        struct.pack_into("!II", pkt, 40, sec, frac)
        try:
            s.sendto(pkt, addr)
            data, _ = s.recvfrom(512)
        except OSError:
            return None
        t4 = time.time()
    if len(data) < 48:
        return None
    s2, f2, s3, f3 = struct.unpack("!IIII", data[32:48])
    t2 = s2 - NTP_DELTA + f2 / (1 << 32)
    t3 = s3 - NTP_DELTA + f3 / (1 << 32)
    offset = ((t2 - t1) + (t3 - t4)) / 2
    rtt = (t4 - t1) - (t3 - t2)
    return offset * 1e3, rtt * 1e3


def sntp_sample(servers, n: int = 5) -> dict | None:
    best = None
    for srv in servers:
        for _ in range(n):
            r = sntp_query(srv)
            if r and (best is None or r[1] < best[1]):
                best = (r[0], r[1], srv)
    if best is None:
        return None
    off, rtt, srv = best
    return {
        "source": "sntp",
        "ref_name": srv,
        "offset_ms": off,
        "rtt_ms": rtt,
        "bound_ms": rtt / 2,
        "correction_ms": off,
    }


class ClockMonitor:
    def __init__(self, cfg: dict) -> None:
        c = cfg["clock"]
        self.mode = c.get("mode", "auto")
        self.max_error_ms = float(c.get("max_error_ms", 1.0))
        self.servers = c.get("sntp_servers") or ["time.google.com"]
        self.last: dict | None = None
        self._last_sntp = 0.0

    def sample(self) -> dict:
        """Blocking; call via asyncio.to_thread from the event loop."""
        s = None
        if self.mode == "none":
            s = {"source": "none", "bound_ms": None, "correction_ms": 0.0}
        if s is None and self.mode in ("auto", "chrony"):
            s = chrony_sample()
        if s is None and self.mode in ("auto", "sntp"):
            # SNTP is cheap but not free; refresh at most every 5 minutes
            if self.last and self.last.get("source") == "sntp" and time.monotonic() - self._last_sntp < 300:
                s = dict(self.last)
                s["reused"] = True
            else:
                s = sntp_sample(self.servers)
                self._last_sntp = time.monotonic()
        if s is None:
            s = {"source": "unavailable", "bound_ms": None, "correction_ms": 0.0}
        s["phc"] = phc_devices()
        s["sample_wall_ns"] = time.time_ns()
        self.last = s
        return s

    def ok(self, s: dict | None) -> bool:
        if s is None:
            return False
        if s.get("source") == "none":
            return True  # explicitly unchecked
        b = s.get("bound_ms")
        return b is not None and b <= self.max_error_ms

    async def wait_converged(self, wait_s: float) -> dict:
        deadline = time.monotonic() + wait_s
        while True:
            s = await asyncio.to_thread(self.sample)
            if self.ok(s) or s.get("source") in ("sntp", "unavailable", "none") or time.monotonic() >= deadline:
                return s
            log.info("waiting for chrony: bound %.3f ms", s.get("bound_ms") or float("nan"))
            await asyncio.sleep(2)
