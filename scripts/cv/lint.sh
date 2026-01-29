#!/bin/bash -eux

set -o pipefail

for MOD in $(find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'); do
    # Change into the module directory
    cd $MOD

    # Run golangci-lint using the top-level config
    golangci-lint run --config ../.golangci.yml --timeout=10m

    # Validate module path matches version in CHANGES.md (MB-70391)
    ../scripts/cv/check-module-version.sh .

    # Return to the parent directory
    cd ..
done
