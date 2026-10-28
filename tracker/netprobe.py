"""Network probes (SPEC §5.6): TCP connect time to every resolved IP of each venue host, WS ping RTT per socket.

Also usable standalone: `python -m tracker.netprobe [--config config.yaml]` prints min/median connect time per IP.
"""

from __future__ import annotations

import argparse
import socket
import statistics
import time
from urllib.parse import urlparse


def hosts_from_cfg(cfg: dict) -> list[tuple[str, int]]:
    seen = []
    for key in ("ws_v1", "ws_v2", "binance_ws", "rest"):
        u = urlparse(cfg["endpoints"][key])
        port = u.port or (443 if u.scheme in ("wss", "https") else 80)
        hp = (u.hostname, port)
        if u.hostname and hp not in seen:
            seen.append(hp)
    return seen


def resolve(host: str, port: int) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        return []
    return sorted({i[4][0] for i in infos})


def tcp_connect_ms(ip: str, port: int, timeout: float = 3.0) -> float | None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        t0 = time.perf_counter_ns()
        s.connect((ip, port))
        return (time.perf_counter_ns() - t0) / 1e6
    except OSError:
        return None
    finally:
        s.close()


def probe(hosts, n: int = 5) -> list[dict]:
    """Blocking; the tracker runs it in a thread so the event loop is not stalled."""
    out = []
    for host, port in hosts:
        for ip in resolve(host, port):
            samples = [tcp_connect_ms(ip, port) for _ in range(n)]
            ok = [x for x in samples if x is not None]
            out.append({
                "type": "netprobe", "host": host, "ip": ip, "port": port, "tcp_ms": ok,
                "failures": n - len(ok),
                "min_ms": min(ok) if ok else None,
                "median_ms": statistics.median(ok) if ok else None,
            })
    return out


def main() -> None:
    from . import config as config_mod

    ap = argparse.ArgumentParser(description="TCP connect-time probe to every venue IP")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("-n", type=int, default=5)
    args = ap.parse_args()
    cfg = config_mod.load(args.config)
    for r in probe(hosts_from_cfg(cfg), args.n):
        mn = f"{r['min_ms']:.3f}" if r["min_ms"] is not None else "-"
        md = f"{r['median_ms']:.3f}" if r["median_ms"] is not None else "-"
        print(f"{r['host']:<40} {r['ip']:<16} min {mn:>9} ms  median {md:>9} ms  failures {r['failures']}")


if __name__ == "__main__":
    main()
