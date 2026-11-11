#!/usr/bin/env python3
"""Tag integrity checker for the debian-observer repo.

Mechanical gates:

1. Every expected tag must exist.
2. Every tag must be an ancestor of the given branch tip (a tag
   pointing outside the current history is orphaned - a tag left
   behind by a rebase).
3. Every tag must point at a commit whose subject matches its
   expected subject pattern (a tag that names the end of a task
   must sit on the commit that ends that task).

Non-mechanical (whether the tag's subject pattern SHOULD be that
one) is review material.

Usage:
    check_tags.py [--config FILE] BRANCH

Config defaults: the R2 tag set. --config accepts a JSON file:
    {"0.8rc": ["record execution benchmarks"], ...}
(patterns are regex, searched in the subject of the tagged commit).
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

DEFAULT_EXPECTED = {
    "0.8rc": ["record execution benchmarks"],
    "R2T1": ["Add R2 spec"],
    "R2T2": ["R2T2 ends at the events-dir default"],
    "R2T3": ["dependencies and popcon eventizers"],
    "R2T4": ["reverse table"],
}


def git(args):
    return subprocess.run(["git"] + args, capture_output=True,
                           text=True).stdout


def check(branch, expected):
    errors, warns = [], []
    for tag, patterns in expected.items():
        r = subprocess.run(["git", "rev-parse",
                             f"{tag}^{{commit}}"],
                            capture_output=True, text=True)
        if r.returncode != 0:
            errors.append(f"{tag}: MISSING")
            continue
        c = r.stdout.strip()
        # 2. ancestor of the branch
        anc = subprocess.run(["git", "merge-base", "--is-ancestor",
                              c, branch])
        if anc.returncode != 0:
            errors.append(f"{tag}: ORPHANED ({c[:7]} is not an "
                          f"ancestor of {branch})")
            continue
        # 3. subject match
        subject = git(["log", "--format=%s", "-1", c]).strip()
        if not any(re.search(p, subject) for p in patterns):
            errors.append(f"{tag}: subject mismatch: {subject[:60]} "
                           f"(expected one of: {patterns})")
    for e in errors:
        print("ERROR:", e)
    print(f"{len(expected)} tags checked: {len(errors)} errors")
    return 1 if errors else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("branch")
    ap.add_argument("--config", default=None)
    a = ap.parse_args()
    expected = DEFAULT_EXPECTED
    if a.config:
        with open(a.config) as fh:
            expected = json.load(fh)
    sys.exit(check(a.branch, expected))


if __name__ == "__main__":
    main()