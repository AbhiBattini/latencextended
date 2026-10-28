"""Archive writer, decoupled from the event loop.

The receive path does `Writer.put(tuple)` (non-blocking; drop-and-count if the bounded queue is full). A background
thread serialises records with orjson (the venue message is embedded verbatim via `orjson.Fragment`), compresses them
with zstd and rotates the file every UTC hour. A zstd frame is closed every `flush_s` seconds so even a `.partial`
file from a crashed run is readable up to the last frame.

Layout of a run directory:
    raw/raw-YYYYmmddTHH-<start_ns>.jsonl.zst   per-message records (SPEC §4.3)
    meta.jsonl                                 run metadata, connects, gaps, clock, net probes, ... (one JSON/line)
    minutes.jsonl                              live per-minute summaries (SPEC §4.4)
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from pathlib import Path

import orjson
import zstandard

log = logging.getLogger("writer")

FIELDS = (
    "feed", "mkt", "conn_id", "topic", "recv_wall_ns", "recv_mono_ns", "venue_ts_ms", "match_T_ms", "seq",
    "seq_gap", "is_catchup", "is_backfill", "warm", "proc_us",
)
_META = object()
_STOP = object()


def _fragment(raw):
    # the receive path already parsed the message: bytes = valid JSON (embed verbatim), str = not JSON
    return orjson.Fragment(raw) if isinstance(raw, bytes) else raw


class Writer:
    def __init__(self, run_dir: Path, maxsize: int = 500_000, flush_s: float = 30.0, level: int = 3) -> None:
        self.run_dir = Path(run_dir)
        (self.run_dir / "raw").mkdir(parents=True, exist_ok=True)
        self.q: queue.Queue = queue.Queue(maxsize=maxsize)
        self.drops = 0
        self.written = 0
        self.bytes = 0
        self.flush_s = flush_s
        self.level = level
        self.start_ns = time.time_ns()
        self._meta_f = open(self.run_dir / "meta.jsonl", "ab")
        self._thread = threading.Thread(target=self._run, name="writer", daemon=True)
        self._thread.start()

    # --- producer side (event loop) ---
    def put(self, rec: tuple) -> None:
        try:
            self.q.put_nowait(rec)
        except queue.Full:
            self.drops += 1

    def meta(self, obj: dict) -> None:
        obj.setdefault("wall_ns", time.time_ns())
        try:
            self.q.put_nowait((_META, obj))
        except queue.Full:
            self.drops += 1

    def close(self, timeout: float = 60.0) -> None:
        self.q.put((_STOP, None))
        self._thread.join(timeout)

    # --- consumer thread ---
    def _open(self, hour: str):
        name = f"raw-{hour}-{self.start_ns}.jsonl.zst"
        path = self.run_dir / "raw" / (name + ".partial")
        f = open(path, "ab")
        w = zstandard.ZstdCompressor(level=self.level).stream_writer(f, closefd=False)
        return path, f, w

    def _close(self, path, f, w) -> None:
        w.flush(zstandard.FLUSH_FRAME)
        w.close()
        f.close()
        os.replace(path, str(path)[: -len(".partial")])

    def _run(self) -> None:
        cur_hour = None
        path = f = w = None
        last_flush = time.monotonic()
        dumps = orjson.dumps
        while True:
            try:
                item = self.q.get(timeout=1.0)
            except queue.Empty:
                item = None
            if item is not None and item[0] is _STOP:
                break
            if item is not None and item[0] is _META:
                self._meta_f.write(dumps(item[1], option=orjson.OPT_SERIALIZE_NUMPY) + b"\n")
                self._meta_f.flush()
            elif item is not None:
                rec = dict(zip(FIELDS, item))
                rec["raw"] = _fragment(item[-1])
                hour = time.strftime("%Y%m%dT%H", time.gmtime(rec["recv_wall_ns"] // 1_000_000_000))
                if hour != cur_hour:
                    if w is not None:
                        self._close(path, f, w)
                    path, f, w = self._open(hour)
                    cur_hour = hour
                line = dumps(rec) + b"\n"
                w.write(line)
                self.written += 1
                self.bytes += len(item[-1])
            now = time.monotonic()
            if w is not None and now - last_flush >= self.flush_s:
                w.flush(zstandard.FLUSH_FRAME)
                f.flush()
                last_flush = now
        if w is not None:
            self._close(path, f, w)
        self._meta_f.close()


def iter_raw(run_dir: Path):
    """Yield (segment_start_ns, record) over every raw file of a run, in order (finished and .partial files)."""
    files = sorted((Path(run_dir) / "raw").glob("raw-*.jsonl.zst*"))
    # order by hour, then by process start
    for p in files:
        parts = p.name.split(".")[0].split("-")
        start = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        with open(p, "rb") as fh:
            reader = zstandard.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
            buf = b""
            while True:
                try:
                    chunk = reader.read(1 << 20)
                except zstandard.ZstdError:
                    log.warning("truncated archive %s (crash?) - stopping at last complete frame", p.name)
                    chunk = b""
                if not chunk:
                    break
                buf += chunk
                lines = buf.split(b"\n")
                buf = lines.pop()
                for ln in lines:
                    if ln:
                        yield start, ln
            if buf.strip():
                try:
                    orjson.loads(buf)
                    yield start, buf
                except orjson.JSONDecodeError:
                    pass


def read_meta(run_dir: Path) -> list[dict]:
    p = Path(run_dir) / "meta.jsonl"
    out = []
    if p.exists():
        for ln in p.read_bytes().splitlines():
            try:
                out.append(orjson.loads(ln))
            except orjson.JSONDecodeError:
                pass
    return out
