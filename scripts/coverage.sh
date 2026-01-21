#!/usr/bin/env bash
set -euo pipefail

EXCLUDE_REGEX='/cmd/|/internal/api/types|/internal/api/server|internal/slurmclient'
PKGS=$(go list ./... | grep -vE "$EXCLUDE_REGEX")

go test $PKGS -cover -coverprofile=coverage.out

go tool cover -func=coverage.out
