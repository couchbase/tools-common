#!/bin/bash -e

for MOD in $(find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'); do
    # Change into the module directory
    cd $MOD

    # Run golangci-lint using the top-level config
    golangci-lint run --config ../.golangci.yml

    # Return to the parent directory
    cd ..
done
