#!/usr/bin/env python

import graphlib
import subprocess
import sys


class SubModule():
    """Wrapper type which implements some helper functions required for topological sorting and version bumping"""
    def __init__(self, name: str):
        self.name = name.removeprefix("github.com/couchbase/tools-common/")
        self._bumped = False

    @property
    def bumped(self):
        return self._bumped

    def bump(self):
        self._bumped = True

    def dependencies(self):
        lines = self._read_mod(f"{self.name}/go.mod")
        deps = [dep for dep in lines[1:] if "tools-common/" in dep]
        return [SubModule(dep.split(" ")[0]) for dep in deps]

    def __repr__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    @classmethod
    def _read_mod(cls, mod: str) -> [str]:
        with open(mod, 'r', encoding="UTF-8") as file:
            return [line.strip() for line in file if line.strip() != ""]


def order(mods):
    """Returns a topological ordering of the given sub-modules.

    This is the order in which modules should have their versions bumped.
    """
    sorter = graphlib.TopologicalSorter()

    for mod in mods:
        sorter.add(mod, *mod.dependencies())

    return list(sorter.static_order())


def bump_dependencies(module, all_modules):
    """Bump any dependencies that are reliant on 'module'"""
    # Don't bother checking any modules prior to the one being bumped
    potentials = all_modules[all_modules.index(module):]

    # Bump the dependency if any of its sub-dependencies have been bumped
    for (idx, mod) in enumerate(all_modules):
        bump_dependency(potentials[:idx], mod)

    return all_modules


def bump_dependency(previous, current):
    """Bumps the current module if any of the previous - dependent - modules have been bumped"""
    dependencies = current.dependencies()

    # If any sub-dependencies have been bumped, this one should be as well
    if len([mod for mod in previous if mod in dependencies and mod.bumped]) != 0:
        current.bump()


def main():
    # Validate that we have all the required arguments
    if len(sys.argv) != 2:
        sys.exit(f"Error: Expected {sys.argv[0]} <module>")

    # The module being bumped
    target = sys.argv[1]

    # Validate we're bumping the version for a known module
    all_modules = subprocess.check_output("find . -name 'go.mod' | xargs dirname | grep -v 'scripts' | tr -d './'",
                                          stderr=subprocess.STDOUT,
                                          shell=True)

    all_modules = all_modules.decode("utf-8").strip().splitlines()

    if not target in all_modules:
        sys.exit(f"Error: Unknown module '{target}'")

    # Create a sub-module for the target
    module = SubModule(target)

    # Bump the target module as that's a given
    module.bump()

    # Create sub-modules
    mods = [module] + [SubModule(mod) for mod in all_modules if mod != target]

    # Create the topological order and extract the module names
    mods = order(mods)

    # Bump any dependent sub-modules
    bumped = bump_dependencies(module, mods)

    # Filter out those that aren't changing
    filtered = [mod for mod in bumped if mod.bumped]

    # Display the modules being bumped
    print(", ".join([mod.name for mod in filtered]))


if __name__ == "__main__":
    main()
