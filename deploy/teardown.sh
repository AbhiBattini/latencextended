#!/usr/bin/env bash
# Terminate every instance tagged Project=ext-latency-tracker.
#   deploy/teardown.sh                 # instances only
#   deploy/teardown.sh --all           # also the IAM role/instance profile and the security group
#   deploy/teardown.sh --all --bucket  # ... and delete the results bucket with everything in it (asks first)
set -euo pipefail
REGION="${AWS_REGION:-ap-northeast-1}"
export AWS_DEFAULT_REGION="$REGION"
ROLE="ext-tracker-role"
SG_NAME="ext-tracker-sg"
ALL=0
DEL_BUCKET=0
BUCKET=""
while [ $# -gt 0 ]; do
  case "$1" in
    --all) ALL=1; shift ;;
    --bucket) DEL_BUCKET=1; if [ $# -gt 1 ] && [[ "$2" != --* ]]; then BUCKET="$2"; shift; fi; shift ;;
    *) echo "unknown option $1" >&2; exit 1 ;;
  esac
done

IDS=$(aws ec2 describe-instances --filters Name=tag:Project,Values=ext-latency-tracker \
      Name=instance-state-name,Values=pending,running,stopping,stopped \
      --query "Reservations[].Instances[].InstanceId" --output text)
if [ -n "$IDS" ]; then
  echo "terminating: $IDS"
  # shellcheck disable=SC2086
  aws ec2 terminate-instances --instance-ids $IDS >/dev/null
  # shellcheck disable=SC2086
  aws ec2 wait instance-terminated --instance-ids $IDS
else
  echo "no tracker instances running"
fi

[ "$ALL" = 1 ] || exit 0

if aws iam get-instance-profile --instance-profile-name "$ROLE" >/dev/null 2>&1; then
  aws iam remove-role-from-instance-profile --instance-profile-name "$ROLE" --role-name "$ROLE" 2>/dev/null || true
  aws iam delete-instance-profile --instance-profile-name "$ROLE"
fi
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam delete-role-policy --role-name "$ROLE" --policy-name ext-tracker-results 2>/dev/null || true
  aws iam detach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore 2>/dev/null || true
  aws iam delete-role --role-name "$ROLE"
  echo "deleted IAM role/profile $ROLE"
fi
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC" Name=group-name,Values="$SG_NAME" \
     --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || echo None)
if [ "$SG" != "None" ]; then
  aws ec2 delete-security-group --group-id "$SG" && echo "deleted security group $SG"
fi

if [ "$DEL_BUCKET" = 1 ]; then
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
  BUCKET="${BUCKET:-ext-latency-tracker-${ACCOUNT}-apne1}"
  read -r -p "Delete s3://$BUCKET and ALL results in it? type the bucket name to confirm: " ans
  if [ "$ans" = "$BUCKET" ]; then
    aws s3 rb "s3://$BUCKET" --force
  else
    echo "bucket kept"
  fi
fi
