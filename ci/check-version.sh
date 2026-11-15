#!/usr/bin/env bash
# Fail unless the release tag (e.g. v0.1.0) matches Version in internal/temporal/version.go.
set -euo pipefail
tag="${1:?usage: check-version.sh <tag>}"
want="${tag#v}"
have="$(sed -n -E 's/^const Version = "([^"]+)"/\1/p' internal/temporal/version.go | head -1)"
[ "$want" = "$have" ] || { echo "tag $tag != internal/temporal/version.go version $have" >&2; exit 1; }
echo "version OK: $have"
