"""Upload a run folder to S3: AWS CLI if present, else boto3. summary.json goes last so a poller that waits for it
sees a complete folder."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

log = logging.getLogger("upload")


def upload_run(run_dir: Path, bucket: str, prefix: str) -> bool:
    run_dir = Path(run_dir)
    dest = f"s3://{bucket}/{prefix}/"
    if shutil.which("aws"):
        try:
            subprocess.run(["aws", "s3", "sync", str(run_dir), dest, "--exclude", "summary.json", "--only-show-errors"],
                           check=True, timeout=1800)
            if (run_dir / "summary.json").exists():
                subprocess.run(["aws", "s3", "cp", str(run_dir / "summary.json"), dest + "summary.json",
                                "--only-show-errors"], check=True, timeout=300)
            log.info("uploaded %s to %s", run_dir, dest)
            return True
        except (subprocess.SubprocessError, OSError) as e:
            log.error("aws s3 sync failed: %s", e)
            return False
    try:
        import boto3
    except ImportError:
        log.error("s3_bucket set but neither the aws CLI nor boto3 is available; results stay in %s", run_dir)
        return False
    s3 = boto3.client("s3")
    files = sorted(p for p in run_dir.rglob("*") if p.is_file())
    files.sort(key=lambda p: p.name == "summary.json")
    for p in files:
        s3.upload_file(str(p), bucket, f"{prefix}/{p.relative_to(run_dir).as_posix()}")
    log.info("uploaded %d files to %s", len(files), dest)
    return True
