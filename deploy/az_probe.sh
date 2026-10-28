#!/usr/bin/env bash
# Optional AZ check (SPEC §6.2): one small instance per Tokyo AZ ID runs a 10-minute depth=1 + v2 capture with
# network probes, then terminates. Prints min TCP connect time per Extended IP and the v2 transport p01 per AZ.
#   deploy/az_probe.sh [--instance-type t3.small] [--minutes 10] [any launch.sh option]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
TYPE="t3.small"
MINUTES=10
PASS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --instance-type) TYPE="$2"; shift 2 ;;
    --minutes) MINUTES="$2"; shift 2 ;;
    *) PASS+=("$1"); shift ;;
  esac
done
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
AZS=(apne1-az1 apne1-az2 apne1-az4)
for az in "${AZS[@]}"; do
  "$HERE/launch.sh" --az-id "$az" --instance-type "$TYPE" --test-minutes "$MINUTES" --markets HYPE-USD \
    --tracker-args "--only-feeds ext_d1_a,ext_rpc_d1" --run-id "azprobe-$az-$STAMP" --no-wait ${PASS[@]+"${PASS[@]}"} &
done
wait

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ext-latency-tracker-${ACCOUNT}-apne1"
for ((i = 0; i < ${#PASS[@]}; i++)); do if [ "${PASS[$i]}" = "--bucket" ]; then BUCKET="${PASS[$((i + 1))]}"; fi; done

echo "waiting for the three probes (~$((MINUTES + 12)) min)..."
DEADLINE=$(( $(date +%s) + (MINUTES + 30) * 60 ))
for az in "${AZS[@]}"; do
  key="azprobe-$az-$STAMP/summary.json"
  until aws s3api head-object --bucket "$BUCKET" --key "$key" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$DEADLINE" ] || { echo "timed out waiting for $key"; break; }
    sleep 30
  done
  mkdir -p "$ROOT/results/azprobe-$az-$STAMP"
  aws s3 cp "s3://$BUCKET/$key" "$ROOT/results/azprobe-$az-$STAMP/summary.json" --only-show-errors || true
done

python3 - "$ROOT/results" "$STAMP" "${AZS[@]}" <<'PY'
import json, sys
from pathlib import Path
root, stamp, azs = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
print(f"\n{'AZ ID':<11}{'instance AZ':<18}{'Extended IP':<18}{'min TCP ms':>11}{'v2 transport p01':>18}")
best = None
for az in azs:
    p = root / f"azprobe-{az}-{stamp}" / "summary.json"
    if not p.exists():
        print(f"{az:<11}(no result)")
        continue
    s = json.loads(p.read_text())
    f = s["markets"].get("HYPE-USD", {}).get("feeds", {})
    tp = (f.get("ext_rpc_d1", {}).get("transport") or {}).get("p01")
    for t in s["network"]["tcp"]:
        if "extended" not in t["host"]:
            continue
        print(f"{az:<11}{(s['run']['host'].get('az') or '-'):<18}{t['ip']:<18}{t['min_ms'] or float('nan'):>11.3f}"
              f"{tp if tp is not None else float('nan'):>18.2f}")
        if t["min_ms"] is not None and (best is None or t["min_ms"] < best[0]):
            best = (t["min_ms"], az, t["ip"])
if best:
    print(f"\nlowest TCP connect: {best[0]:.3f} ms from {best[1]} to {best[2]}")
PY
