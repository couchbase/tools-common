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

# Provide the commands to the user as it's less destructive
print(f"git tag -a {module}/{version}")
print(f"git push gerrit {version} --no-verify")
