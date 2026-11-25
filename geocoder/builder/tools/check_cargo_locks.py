#!/usr/bin/env python3
"""Checks that Cargo lockfiles agree with a base one.

Usage: check_cargo_locks.py <base Cargo.lock> <Cargo.lock>...

Every package each later lockfile holds must be in the base at the same version and source; a
lockfile may hold fewer. The test portal's server, query-portal, is a bin of place-search, built
with geocoder/search/Cargo.lock, but its sources are tested in query-server's tests, built with
geocoder/server/Cargo.lock (CI's server-tests): with search/Cargo.lock agreeing with
server/Cargo.lock, the binary links the very dependencies its tests ran with. A dependency
updated in one lockfile alone (an update of the one manifest that names it) fails this until the
other has it too. Exits 1, naming each package that differs, else 0.
"""
import sys
import tomllib


def packages(path):
    """The (name, version, source) of each package of the lockfile at `path`; source None for a
    path package."""
    with open(path, "rb") as f:
        lock = tomllib.load(f)
    return {(p["name"], p["version"], p.get("source")) for p in lock.get("package", [])}


def main(argv):
    if len(argv) < 2:
        print(__doc__.strip().splitlines()[2])
        return 2
    base = packages(argv[0])
    versions = {}
    for name, version, source in base:
        versions.setdefault(name, []).append(f"{version} ({source or 'path'})")
    failed = False
    for path in argv[1:]:
        for name, version, source in sorted(packages(path) - base, key=lambda p: (p[0], p[1], p[2] or "")):
            failed = True
            held = ", ".join(sorted(versions.get(name, []))) or "none"
            print(f"{path}: {name} {version} ({source or 'path'}) is not in {argv[0]}, which holds {name} {held}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
