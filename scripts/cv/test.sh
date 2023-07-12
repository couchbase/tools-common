#!/bin/bash -e

# Allow users to pass in a target directory where output will be stored.
#
# - $TARGET/tests.xml
# - $TARGET/coverage.xml
#
# Uses the current working directory by default.
TARGET=$1

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

# Create sub-directories test reports/coverage
REPORTS=$TEMP/reports
COVERAGE=$TEMP/coverage

mkdir $REPORTS
mkdir $COVERAGE

for MOD in $(find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'); do
    # Change into the module directory
    cd $MOD

    # Start from a clean slate
    go clean -testcache

    COV_FILE=$COVERAGE/$MOD.out
    REP_FILE=$REPORTS/$MOD.raw

    # Run testing outputting to 'stdout' and to a module specific file
    2>&1 go test -v -timeout=15m -count=1 -coverprofile=$COV_FILE ./... | tee $REP_FILE

    # Convert the module specific file into JSON
    gocov convert $COV_FILE > ${COV_FILE%.out}.json

    # Return to the parent directory
    cd ..
done

# Convert the test output to XML
cat $REPORTS/*.raw | go-junit-report > $TARGET/tests.xml

# Merge the coverage files
jq -n '{ Packages: [ inputs.Packages ] | add }' $(find $COVERAGE -name '*.json') | gocov-xml > $TARGET/coverage.xml
