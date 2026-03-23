#!/usr/bin/env bash
# LOC & commit statistics for devstudio-proxy
set -euo pipefail
trap '' PIPE

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "============================================="
echo "  devstudio-proxy — Source Code & Commit Stats"
echo "============================================="
echo ""

# Commits
total_commits=$(git -C "$REPO_ROOT" rev-list --count HEAD)
first_commit=$(git -C "$REPO_ROOT" log --reverse --format="%ai" | head -1 || true)
latest_commit=$(git -C "$REPO_ROOT" log -1 --format="%ai")
echo "Commits:        $total_commits"
echo "First commit:   $first_commit"
echo "Latest commit:  $latest_commit"
echo ""

# Go source LOC
go_files=$(find "$REPO_ROOT/src" -type f -name "*.go" -not -name "*_test.go" | wc -l | tr -d ' ')
go_loc=$(find "$REPO_ROOT/src" -type f -name "*.go" -not -name "*_test.go" | xargs cat 2>/dev/null | wc -l | tr -d ' ')

# Go test LOC
test_files=$(find "$REPO_ROOT/src" -type f -name "*_test.go" | wc -l | tr -d ' ')
test_loc=$(find "$REPO_ROOT/src" -type f -name "*_test.go" | xargs cat 2>/dev/null | wc -l | tr -d ' ')

# Config / build files
config_loc=$(find "$REPO_ROOT" -type f \( -name "Makefile" -o -name "*.yml" -o -name "*.yaml" -o -name "*.rb" \) -not -path "*/.git/*" | xargs cat 2>/dev/null | wc -l | tr -d ' ')

total_loc=$((go_loc + test_loc + config_loc))

echo "--- Lines of Code ---"
echo ""
printf "  %-25s %8s files  %10s LOC\n" "Go source" "$go_files" "$go_loc"
printf "  %-25s %8s files  %10s LOC\n" "Go tests" "$test_files" "$test_loc"
printf "  %-25s %8s        %10s LOC\n" "Config/CI/Build" "" "$config_loc"
echo "  -------------------------------------------------"
printf "  %-25s %8s        %10s LOC\n" "TOTAL" "" "$total_loc"
echo ""
