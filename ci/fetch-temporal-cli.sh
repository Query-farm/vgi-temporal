#!/usr/bin/env bash
# Download the pinned Temporal CLI (it embeds `temporal server start-dev`, the
# in-memory dev server the SQL tests run against) into .tools/bin/temporal and
# verify its SHA-256 against the checksums pinned below.
#
#   ci/fetch-temporal-cli.sh            # prints the path of the binary
#
# Bump TEMPORAL_CLI_VERSION together with the checksums (copy them from the
# release's checksums.txt).
set -euo pipefail

TEMPORAL_CLI_VERSION="${TEMPORAL_CLI_VERSION:-1.10.1}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$REPO/.tools/bin/temporal"

checksum() {
  case "$1" in
    darwin_arm64) echo 94f7f982b26438e8abbde6000ba471b694be6a2413b31f6162f87a9f9c48c738 ;;
    darwin_amd64) echo 8ec6c513ad2177de4eb15d2cb17a4fe5070e6078afd7f80bdfcc37fae459d3c4 ;;
    linux_amd64)  echo 4c1500e2ce2d4de136e5fe8852d9b7f82eb62f19b6b383b3e0c040519a9d20b1 ;;
    linux_arm64)  echo 2f748e9411b37c9cabb57f1f08e37765388c12b5534432c7aea28b593df9145d ;;
    *) return 1 ;;
  esac
}

if [ -x "$DEST" ] && "$DEST" --version 2>/dev/null | grep -q "temporal version $TEMPORAL_CLI_VERSION "; then
  echo "$DEST"; exit 0
fi

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in x86_64|amd64) arch=amd64 ;; arm64|aarch64) arch=arm64 ;; esac
platform="${os}_${arch}"
want="$(checksum "$platform")" || { echo "no pinned Temporal CLI for $platform" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
asset="temporal_cli_${TEMPORAL_CLI_VERSION}_${platform}.tar.gz"
curl -fsSL --retry 3 -o "$tmp/$asset" \
  "https://github.com/temporalio/cli/releases/download/v${TEMPORAL_CLI_VERSION}/${asset}"
if command -v sha256sum >/dev/null; then got="$(sha256sum "$tmp/$asset" | cut -d' ' -f1)"
else got="$(shasum -a 256 "$tmp/$asset" | cut -d' ' -f1)"; fi
[ "$got" = "$want" ] || { echo "checksum mismatch for $asset: got $got want $want" >&2; exit 1; }
mkdir -p "$tmp/x" "$(dirname "$DEST")"
tar -xzf "$tmp/$asset" -C "$tmp/x" temporal
mv "$tmp/x/temporal" "$DEST"
chmod +x "$DEST"
echo "$DEST"
