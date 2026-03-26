#!/usr/bin/env bash
# scripts/release.sh — safely tag a release from the tip of main
# Usage: ./scripts/release.sh v0.13.0
set -euo pipefail

VERSION="${1:-}"

# Validate argument
if [[ -z "${VERSION}" ]]; then
  git fetch origin --tags --quiet 2>/dev/null
  LATEST="$(git tag --sort=-v:refname | head -n1)"
  LATEST="${LATEST:-none}"
  echo "Latest version: ${LATEST}" >&2
  echo "Run 'git pull origin main' to ensure you're up to date before tagging." >&2
  echo "Usage: $0 vMAJOR.MINOR.PATCH" >&2
  exit 1
fi

if ! [[ "${VERSION}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: version must match vMAJOR.MINOR.PATCH (got '${VERSION}')" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

# Must be on main
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${CURRENT_BRANCH}" != "main" ]]; then
  echo "Error: you must be on the 'main' branch (currently on '${CURRENT_BRANCH}')" >&2
  exit 1
fi

# Working tree must be clean
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Error: working tree is dirty — commit or stash changes first" >&2
  exit 1
fi

# Must be up to date with origin/main
git fetch origin main --quiet
LOCAL="$(git rev-parse HEAD)"
REMOTE="$(git rev-parse origin/main)"
if [[ "${LOCAL}" != "${REMOTE}" ]]; then
  echo "Error: local main (${LOCAL:0:7}) differs from origin/main (${REMOTE:0:7})" >&2
  echo "Run: git pull --rebase origin main" >&2
  exit 1
fi

# Tag must not already exist
if git rev-parse "${VERSION}" &>/dev/null; then
  echo "Error: tag ${VERSION} already exists" >&2
  exit 1
fi

echo "Tagging ${VERSION} at $(git rev-parse --short HEAD) on branch main"
git tag "${VERSION}"
git push origin "${VERSION}"
echo "Done. Workflow triggered at: https://github.com/devstudio-live/devstudio-proxy/actions"
