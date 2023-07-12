#!/bin/bash -e

for MOD in $(find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'); do
    # Change into the module directory
    cd $MOD

    # Run all the benchmarks skipping any test cases by providing a regular
    # expression that doesn't match any test cases.
    go test -timeout=15m -count=1 -run='^\044' -bench=Benchmark ./...

    # Return to the parent directory
    cd ..
done
