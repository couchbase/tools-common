#!/usr/bin/env python

import sys
import subprocess

import semver

# Validate that we have all the required arguments
if len(sys.argv) != 3:
    sys.exit(f"Error: Expected {sys.argv[0]} <module> <mode>")

# The module being tagged
module = sys.argv[1]

# Validate we're bumping the version for a known module
all_modules = subprocess.check_output("find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'",
                                      stderr=subprocess.STDOUT,
                                      shell=True)

all_modules = all_modules.decode("utf-8").strip().splitlines()

if not module in all_modules:
    sys.exit(f"Error: Unknown module '{module}'")

# The operation being performed e.g. bumping the 'major', 'minor' or 'patch'
mode = sys.argv[2]

# Validate that our mode argument is sane
modes = ["major", "minor", "patch"]
if mode not in modes:
    sys.exit(f"Error: Expected <mode> to be one of {', '.join(modes)}")

# Find the last tag
last = subprocess.check_output(f"git tag | grep {module} | tr -d {module}/v | sort | tail -1",
                               stderr=subprocess.STDOUT,
                               shell=True)

last = last.decode("utf-8").strip()

# Parse the last tag
version = semver.VersionInfo.parse(last)

# Bump the version
if mode == "major":
    version = version.bump_major()
elif mode == "minor":
    version = version.bump_minor()
elif mode == "patch":
    version = version.bump_patch()

# See if the major version is sane, a non-zero exit status means that's not the case
proc = subprocess.run(f"head -1 {module}/go.mod | grep -q /v'{str(version).split('.', 1)[0]}'",
                       stderr=subprocess.STDOUT,
                       shell=True)

# Complain if the major version doesn't seem to match
if version.major > 1 and proc.returncode != 0:
    sys.exit(f"Error: Version in 'go.mod' does no match the target tag version, check versions are correct")

full_version = f"{module}/v{version}"

# Provide the commands to the user as it's less destructive
print(f"git tag -a {full_version}")
print(f"git push gerrit {full_version} --no-verify")
