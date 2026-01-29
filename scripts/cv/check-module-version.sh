#!/bin/bash -eux
#
# Checks that go.mod module path matches the major version in CHANGES.md.
#
# Usage: check-module-version.sh <module-dir>
# Example: check-module-version.sh ./couchbase
#
# Go v2+ modules require "/vN" in the module path. If CHANGES.md says v4.0.0,
# then go.mod must have "module .../v4". This script catches mismatches.
#
set -o pipefail

MODULE_DIR="$1"
CHANGES_FILE="$MODULE_DIR/CHANGES.md"

# Skip if no CHANGES.md (module may not need version tracking)
if [[ ! -f "$CHANGES_FILE" ]]; then
  echo "OK: No CHANGES.md, skipping"
  exit 0
fi

# Get major version from first "## vX.Y.Z" heading (e.g., "## v4.0.1" -> "4")
VERSION_LINE=$(grep -m1 -E '^## v[0-9]+\.[0-9]+\.[0-9]+' "$CHANGES_FILE" || true)
if [[ -z "$VERSION_LINE" ]]; then
  echo "ERROR: No valid semver (vX.Y.Z) found in $CHANGES_FILE"
  exit 1
fi
MAJOR=$(echo "$VERSION_LINE" | sed -E 's/^## v([0-9]+)\..*/\1/')

# Get module path from go.mod
if [[ ! -f "$MODULE_DIR/go.mod" ]]; then
  echo "ERROR: $MODULE_DIR/go.mod not found"
  exit 1
fi
MODULE_PATH=$(awk '/^module / {print $2; exit}' "$MODULE_DIR/go.mod")

# For v2+, module path must end with /vN
if [[ "$MAJOR" -ge 2 ]] && [[ ! "$MODULE_PATH" =~ /v${MAJOR}$ ]]; then
  echo "ERROR: go.mod says '$MODULE_PATH' but CHANGES.md is at v$MAJOR"
  exit 1
fi

echo "OK: $MODULE_PATH (v$MAJOR)"
