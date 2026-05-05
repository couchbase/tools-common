#!/bin/bash -eux

set -o pipefail

# Allow users to pass in a target directory where output will be stored.
#
# - $TARGET/tests.xml
# - $TARGET/coverage.xml
#
# Uses the current working directory by default.
TARGET=${1:-$PWD}

if [[ $TARGET == "" ]]; then
    TARGET=$PWD
fi

# Create somewhere to store files temporarily
TEMP=$(mktemp --directory)

# Ensure that $TEMP is cleaned up
function cleanup() {
    if [[ $TEMP != "" ]]; then
        rm -rf $TEMP
    fi
}

trap cleanup SIGINT EXIT

for MOD in $(find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'); do
    # Change into the module directory
    cd $MOD

    # Start from a clean slate
    go clean -testcache

    COV_FILE=$TARGET/$MOD.out

    # Run testing outputting to 'stdout' and to a module specific file
    2>&1 go test -v -count=1 -coverprofile=$COV_FILE ./...

    # Return to the parent directory
    cd ..
done
