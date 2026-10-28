#!/usr/bin/env bash
# One-command EC2 launch of the tracker in AWS Tokyo, AZ ID apne1-az4 (SPEC §6.3).
#
#   deploy/launch.sh --test              # 15-min test; waits for and prints the latency tables; instance self-terminates
#   deploy/launch.sh --hours 24          # the full measurement run
#
# Options:
#   --test-minutes N       test capture length (default 15)
#   --minutes N            long run length in minutes (alternative to --hours)
#   --az-id ID             AZ *ID* (default apne1-az4; never an AZ name - names differ per account)
#   --instance-type T      default c7i.large (c7g.* / *g.* types use the arm64 AMI)
#   --bucket NAME          results bucket (default ext-latency-tracker-<account>-apne1)
#   --repo URL --branch B  what the instance clones (default: this checkout's origin + current branch)
#   --git-token-param P    SSM SecureString holding a GitHub token, for a private repo
#   --markets A,B          e.g. HYPE-USD,BTC-USD
#   --tracker-args "..."   extra tracker CLI args (e.g. "--only-feeds ext_d1_a,ext_rpc_d1")
#   --run-id ID            default <test|run>-<UTC time>
#   --spot                 use a spot instance
#   --no-wait              --test: don't wait for the result
# Requires python3 and AWS CLI v2 configured for an account that can create EC2, IAM and S3 resources.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REGION="${AWS_REGION:-ap-northeast-1}"
AZ_ID="apne1-az4"
TYPE="c7i.large"
MODE="test"
TEST_MINUTES=15
RUN_MIN=""
BUCKET=""
REPO_URL=""
BRANCH=""
GIT_TOKEN_PARAM=""
MARKETS=""
EXTRA_ARGS=""
RUN_ID=""
SPOT=0
WAIT=1
ROLE="ext-tracker-role"
SG_NAME="ext-tracker-sg"
TAG="ext-latency-tracker"

while [ $# -gt 0 ]; do
  case "$1" in
    --test) MODE="test"; shift ;;
    --test-minutes) MODE="test"; TEST_MINUTES="$2"; shift 2 ;;
    --hours) MODE="run"; RUN_MIN=$(( $2 * 60 )); shift 2 ;;
    --minutes) MODE="run"; RUN_MIN="$2"; shift 2 ;;
    --az-id) AZ_ID="$2"; shift 2 ;;
    --instance-type) TYPE="$2"; shift 2 ;;
    --bucket) BUCKET="$2"; shift 2 ;;
    --repo) REPO_URL="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --git-token-param) GIT_TOKEN_PARAM="$2"; shift 2 ;;
    --markets) MARKETS="$2"; shift 2 ;;
    --tracker-args) EXTRA_ARGS="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --spot) SPOT=1; shift ;;
    --no-wait) WAIT=0; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 1 ;;
  esac
done

command -v aws >/dev/null || { echo "aws CLI v2 is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
export AWS_DEFAULT_REGION="$REGION"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="${BUCKET:-ext-latency-tracker-${ACCOUNT}-apne1}"
if [ -z "$REPO_URL" ]; then
  REPO_URL=$(git -C "$ROOT" remote get-url origin 2>/dev/null || true)
  # the instance clones over HTTPS
  REPO_URL=$(echo "$REPO_URL" | sed -E 's#^git@github.com:#https://github.com/#; s#^http://[^/]+/git/#https://github.com/#')
fi
[ -n "$REPO_URL" ] || { echo "--repo is required (no git origin found)" >&2; exit 1; }
BRANCH="${BRANCH:-$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_ID="${RUN_ID:-${MODE}-${STAMP}}"

if [ "$MODE" = "test" ]; then
  TRACKER_ARGS="--test --test-minutes ${TEST_MINUTES}"
  RUN_MINUTES=$(( TEST_MINUTES + 15 ))   # 15 capture + boot, clock convergence, report, upload
else
  RUN_MIN="${RUN_MIN:-1440}"
  TRACKER_ARGS="--minutes ${RUN_MIN} --require-clock"
  RUN_MINUTES=$(( RUN_MIN + 45 ))        # capture + boot + the end-of-run report over the whole archive
fi
[ -n "$MARKETS" ] && TRACKER_ARGS="$TRACKER_ARGS --markets $MARKETS"
[ -n "$EXTRA_ARGS" ] && TRACKER_ARGS="$TRACKER_ARGS $EXTRA_ARGS"

log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

# ---------------------------------------------------------------- 1. bucket + IAM role / instance profile
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  log "creating bucket s3://$BUCKET"
  aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
fi

NEW_PROFILE=0
if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  log "creating IAM role $ROLE"
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document '{
    "Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
fi
POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:PutObject"],"Resource":"arn:aws:s3:::${BUCKET}/*"},
 {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":"arn:aws:s3:::${BUCKET}"}
 $( [ -n "$GIT_TOKEN_PARAM" ] && echo ",{\"Effect\":\"Allow\",\"Action\":[\"ssm:GetParameter\"],\"Resource\":\"arn:aws:ssm:${REGION}:${ACCOUNT}:parameter/${GIT_TOKEN_PARAM#/}\"}" )
]}
EOF
)
aws iam put-role-policy --role-name "$ROLE" --policy-name ext-tracker-results --policy-document "$POLICY"
if ! aws iam get-instance-profile --instance-profile-name "$ROLE" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "$ROLE" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "$ROLE" --role-name "$ROLE"
  NEW_PROFILE=1
fi

# ---------------------------------------------------------------- 2. security group without inbound rules
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
[ "$VPC" != "None" ] || { echo "no default VPC in $REGION; create one: aws ec2 create-default-vpc" >&2; exit 1; }
SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC" Name=group-name,Values="$SG_NAME" \
     --query "SecurityGroups[0].GroupId" --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --vpc-id "$VPC" --group-name "$SG_NAME" \
       --description "ext-latency-tracker: no inbound, all outbound" --query GroupId --output text)
  log "created security group $SG (no inbound rules)"
fi

# ---------------------------------------------------------------- 3. subnet by AZ *ID* + latest AL2023 AMI
AZ_NAME=$(aws ec2 describe-availability-zones --zone-ids "$AZ_ID" --query "AvailabilityZones[0].ZoneName" --output text)
SUBNET=$(aws ec2 describe-subnets --filters Name=availability-zone-id,Values="$AZ_ID" Name=default-for-az,Values=true \
         --query "Subnets[0].SubnetId" --output text)
if [ "$SUBNET" = "None" ]; then
  log "no default subnet in $AZ_ID ($AZ_NAME); creating one"
  SUBNET=$(aws ec2 create-default-subnet --availability-zone "$AZ_NAME" --query Subnet.SubnetId --output text)
fi
case "$TYPE" in
  *g.*|*gd.*|*gn.*|a1.*) ARCH=arm64 ;;
  *) ARCH=x86_64 ;;
esac
AMI=$(aws ssm get-parameter --name "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${ARCH}" \
      --query Parameter.Value --output text)

# ---------------------------------------------------------------- 4. run-instances
UD=$(mktemp)
trap 'rm -f "$UD"' EXIT
export REPO_URL BRANCH BUCKET RUN_ID TRACKER_ARGS RUN_MINUTES GIT_TOKEN_PARAM REGION
# envsubst restricted to our placeholders (python3: envsubst is often missing, e.g. on macOS)
python3 - "$HERE/cloud-init.yaml" > "$UD" <<'PY'
import os, sys
s = open(sys.argv[1]).read()
for k in ("REPO_URL", "BRANCH", "BUCKET", "RUN_ID", "TRACKER_ARGS", "RUN_MINUTES", "GIT_TOKEN_PARAM", "REGION"):
    s = s.replace("${%s}" % k, os.environ.get(k, ""))
sys.stdout.write(s)
PY

MARKET_OPTS=()
[ "$SPOT" = 1 ] && MARKET_OPTS=(--instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}')
[ "$NEW_PROFILE" = 1 ] && { log "waiting for the new instance profile to propagate"; sleep 15; }

log "launching $TYPE ($ARCH) in $AZ_ID/$AZ_NAME subnet $SUBNET, run $RUN_ID, budget ${RUN_MINUTES} min"
for attempt in 1 2 3 4 5 6; do
  if IID=$(aws ec2 run-instances \
      --image-id "$AMI" --instance-type "$TYPE" --subnet-id "$SUBNET" --associate-public-ip-address \
      --security-group-ids "$SG" --iam-instance-profile Name="$ROLE" \
      --instance-initiated-shutdown-behavior terminate \
      --metadata-options HttpTokens=required,HttpEndpoint=enabled \
      --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3,DeleteOnTermination=true}' \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=$TAG},{Key=Name,Value=ext-tracker-$RUN_ID},{Key=RunId,Value=$RUN_ID}]" \
      ${MARKET_OPTS[@]+"${MARKET_OPTS[@]}"} \
      --user-data "file://$UD" --query "Instances[0].InstanceId" --output text 2>"$UD.err"); then
    break
  fi
  if grep -q "Invalid IAM Instance Profile" "$UD.err" && [ $attempt -lt 6 ]; then
    log "instance profile not visible yet; retrying in 10s"; sleep 10; continue
  fi
  cat "$UD.err" >&2; exit 1
done
rm -f "$UD.err"

cat <<EOF

instance:   $IID  ($TYPE, $AZ_ID)
run id:     $RUN_ID
results:    s3://$BUCKET/$RUN_ID/

connect:    aws ssm start-session --region $REGION --target $IID
logs:       sudo journalctl -u ext-tracker -f          (inside the session; boot log: /var/log/cloud-init-output.log)
fetch:      aws s3 sync s3://$BUCKET/$RUN_ID ./results/$RUN_ID
stop:       deploy/teardown.sh
EOF

[ "$MODE" = "test" ] && [ "$WAIT" = 1 ] || exit 0

# ---------------------------------------------------------------- 5. test mode: wait for the result
log "waiting for s3://$BUCKET/$RUN_ID/summary.json (polling every 30 s, up to 40 min)"
DEADLINE=$(( $(date +%s) + 40 * 60 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if aws s3api head-object --bucket "$BUCKET" --key "$RUN_ID/summary.json" >/dev/null 2>&1; then
    mkdir -p "$ROOT/results/$RUN_ID"
    aws s3 sync "s3://$BUCKET/$RUN_ID" "$ROOT/results/$RUN_ID" --only-show-errors
    echo
    cat "$ROOT/results/$RUN_ID/summary.txt" 2>/dev/null || true
    echo
    echo "report: $ROOT/results/$RUN_ID/report.md"
    python3 - "$ROOT/results/$RUN_ID/summary.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
bad = [c for c in s["checks"] if not c["ok"]]
sys.exit(2 if bad else 0)
PY
    exit $?
  fi
  STATE=$(aws ec2 describe-instances --instance-ids "$IID" --query "Reservations[0].Instances[0].State.Name" --output text 2>/dev/null || echo "?")
  log "instance $STATE; no result yet"
  sleep 30
done
log "no summary.json after 40 min - last console output:"
aws ec2 get-console-output --instance-id "$IID" --latest --output text 2>/dev/null | tail -100
exit 1
