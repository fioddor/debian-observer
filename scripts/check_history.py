#!/usr/bin/env python3
"""History linter for the debian-observer repo.

Mechanical gates (exit non-zero on ERROR; WARNs pass):

1. MVC by subject tag: [R] touches only tests/ (+docs/); [G] touches
   only src/ (+ pyproject.toml: a G that registers an executable
   edits its entrypoint there); [B] unconstrained by directory.
2. Empty commits (0 files changed) are errors: a commit that adds
   nothing is a reconstruction artifact.
3. Message width: subject lines over 72 columns WARN (subjects are
   format-limited); body lines over 72 columns are ERRORS.
4. Development history in the product: references to the project's
   own development (release numbers, planning docs) inside src/ and
   tests/ are errors. The product describes what it is, not when it
   was written. Configurable patterns with explicit exceptions.
5. Parallel artifacts: names like ``*_r2`` (a second version of an
   existing module living beside it) are errors by name pattern.
6. Reconstruction integrity (opt-in, --expect-tree SHA): when a
   history surgery must NOT change the final content, the new tip's
   tree must equal the expected tree - a differing tree means the
   reconstruction dropped or added content.

7. MVC order by subject pair: a [G] commit whose subject matches an [R]
   commit LATER in the chain is an error - the red test must exist before
   the green implementation makes it pass. (A late R is born green.)

Non-mechanical checks (subject scope coherence, body promises vs diff
promises, tests living with the implementation they motivate) are
review-checklist material: this script does not attempt them.

Usage:
    check_history.py BASE..TIP [--expect-tree SHA]

Run before every push; in CI against every PR targeting tdd.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys

MAX_COLS = 72

#: References to the development process that must not appear in the
#: product (src/, tests/). Each: (pattern, reason, exception-contexts).
FORBIDDEN_CONTENT = [
    (r"\bR1\b", "development history in the product",
     ("deferred",)),          # "deferred to R1/R2" is a plan, not a claim
    (r"\bR2\b", "development history in the product",
     ("deferred", "REUSE", "AGPL")),  # licenses mention their own terms
    (r"\bbrief\b", "planning vocabulary in the product", ()),
]
#: A second version of a module living beside the original: evolution
#: happens IN the file, never beside it.
FORBIDDEN_NAME = re.compile(r"_r2\b")


def git(args):
    return subprocess.run(["git"] + args, capture_output=True,
                           text=True).stdout


def check_range(base, tip, expect_tree):
    chain = git(["rev-list", "--reverse", f"{base}..{tip}"]).split()
    errors, warns = [], []
    for h in chain:
        subject = git(["log", "--format=%s", "-1", h]).strip()
        body = git(["log", "--format=%b", "-1", h])
        files = [l for l in
                 git(["diff-tree", "--no-commit-id", "--name-only",
                      "-r", h]).splitlines() if l.strip()]
        tag = subject[1:3] if subject.startswith("[") else "??"

        # 2. empty commits
        if not files:
            errors.append(f"{h[:7]}: EMPTY COMMIT (0 files) "
                          f"{subject[:50]}")

        # 1. MVC directories
        if tag == "R]" and [
                f for f in files
                if not f.startswith(("tests/", "docs/"))]:
            bad = [f for f in files
                   if not f.startswith(("tests/", "docs/"))]
            errors.append(f"{h[:7]}: [R] touches non-test: {bad[:3]}")
        if tag == "G]" and [
                f for f in files
                if not (f.startswith("src/")
                        or f == "pyproject.toml")]:
            bad = [f for f in files
                   if not (f.startswith("src/")
                           or f == "pyproject.toml")]
            errors.append(f"{h[:7]}: [G] touches non-src: {bad[:3]}")

        # 3. message width (subject warns, body gates)
        if len(subject) > MAX_COLS:
            warns.append(f"{h[:7]}: subject {len(subject)} cols (>72)")
        for i, line in enumerate(body.splitlines()):
            if len(line) > MAX_COLS:
                errors.append(f"{h[:7]}: body line {i + 1} "
                               f"{len(line)} cols (>72)")

        # 4+5. forbidden content and names
        for f in files:
            if FORBIDDEN_NAME.search(f):
                errors.append(f"{h[:7]}: parallel artifact: {f}")
            if f.startswith(("src/", "tests/")):
                content = git(["show", f"{h}:{f}"])
                for pat, reason, exc in FORBIDDEN_CONTENT:
                    hit = False
                    for m in re.finditer(pat, content):
                        ctx = content[max(0, m.start() - 40):
                                       m.end() + 40]
                        if not any(e in ctx for e in exc):
                            hit = True
                            break
                    if hit:
                        errors.append(
                            f"{h[:7]}: {reason}: {f}")
                        break

    # 7. MVC order: each [G] must have its matching [R] BEFORE it
    subjects = [git(["log", "--format=%s", "-1", h]).strip() for h in chain]
    for i, s in enumerate(subjects):
        if not s.startswith("[G]"):
            continue
        tail = s[4:].strip()
        for j in range(i + 1, len(subjects)):
            if subjects[j].startswith("[R]") and subjects[j][4:].strip() == tail:
                errors.append(
                    f"{chain[i][:7]}: INVERTED MVC: [G] before its [R] "
                    f"{chain[j][:7]} ({tail[:40]})")
                break

    # 6. reconstruction integrity
    if expect_tree:
        t_new = git(["rev-parse", f"{tip}^{{tree}}"]).strip()
        t_old = git(["rev-parse", f"{expect_tree}^{{tree}}"]).strip()
        if t_new != t_old:
            d = git(["diff", "--stat", expect_tree, tip])
            errors.append("RECONSTRUCTION: final tree differs from "
                          f"expected:\n{d[:500]}")

    for w in warns:
        print("WARN:", w)
    for e in errors:
        print("ERROR:", e)
    print(f"{len(chain)} commits checked: "
          f"{len(errors)} errors, {len(warns)} warnings")
    return 1 if errors else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("range_", metavar="RANGE", help="BASE..TIP")
    ap.add_argument("--expect-tree", default=None,
                    help="commit whose tree the tip must equal "
                         "(content-preserving surgery)")
    a = ap.parse_args()
    base, tip = a.range_.split("..")
    sys.exit(check_range(base, tip, a.expect_tree))


if __name__ == "__main__":
    main()