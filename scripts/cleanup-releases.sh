#!/usr/bin/env bash
# scripts/cleanup-releases.sh — delete old GitHub releases, keeping the latest N
# Usage: ./scripts/cleanup-releases.sh [--keep N] [--dry-run]
#   --keep N     Number of most-recent releases to keep (default: 1)
#   --dry-run    Preview without deleting anything
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

KEEP=1
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep)
      KEEP="${2:?--keep requires a number}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      echo "Usage: $0 [--keep N] [--dry-run]" >&2
      exit 1
      ;;
  esac
done

# Get all release tags, newest first
RELEASES=$(gh release list --limit 100 --json tagName --jq '.[].tagName')
TOTAL=$(echo "${RELEASES}" | grep -c . || true)

if [[ "${TOTAL}" -le "${KEEP}" ]]; then
  echo "Found ${TOTAL} release(s), keeping ${KEEP} — nothing to delete."
  exit 0
fi

KEEPING=$(echo "${RELEASES}" | head -n "${KEEP}")
TO_DELETE=$(echo "${RELEASES}" | tail -n "+$((KEEP + 1))")
DELETE_COUNT=$(echo "${TO_DELETE}" | grep -c . || true)

echo "Keeping (${KEEP}):"
echo "${KEEPING}" | sed 's/^/  /'
echo ""
echo "Deleting (${DELETE_COUNT}):"
echo "${TO_DELETE}" | sed 's/^/  /'
echo ""

if [[ "${DRY_RUN}" == true ]]; then
  echo "Dry run — no releases deleted."
  exit 0
fi

echo "${TO_DELETE}" | while read -r tag; do
  echo "Deleting ${tag}..."
  gh release delete "${tag}" --yes --cleanup-tag
done

echo ""
echo "Done. ${DELETE_COUNT} release(s) deleted."
