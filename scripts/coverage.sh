#!/usr/bin/env bash
set -euo pipefail

SHOW_ZERO_ONLY=false

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --zero)
      SHOW_ZERO_ONLY=true
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

EXCLUDE_REGEX='/cmd/|/internal/api/types|/internal/api/server|internal/domain|internal/slurmclient'
PKGS=$(go list ./... | grep -vE "$EXCLUDE_REGEX")

go test $PKGS -cover -coverprofile=coverage.out

if [[ "$SHOW_ZERO_ONLY" == true ]]; then
  go tool cover -func=coverage.out | awk '$NF=="0.0%"'
else
  go tool cover -func=coverage.out
fi
