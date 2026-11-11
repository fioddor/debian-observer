#!/usr/bin/env python3
"""pdoc regeneration check for the debian-observer repo.

For each commit of a range (or just one): run pdoc over the package
sources and assert it renders without crashing. The technical docs are
generated from module docstrings; a module that breaks pdoc breaks
the docs build.

Usage:
    check_pdoc.py BASE..TIP [--root DIR]
    check_pdoc.py COMMIT [--root DIR]

Runs pdoc in dry-render mode (no output files) over src/debian_observer
and src/prosumers. Exit non-zero on the first crashing commit.
"""
from __future__ import annotations

import argparse
import subprocess
import sys

PACKAGES = ["src/debian_observer", "src/prosumers"]


def check_commit(c, root):
    # render to /tmp per commit; --no-src excludes source view? keep
    # defaults: we only care that rendering does not crash
    env = None
    r = subprocess.run(
        [sys.executable, "-m", "pdoc", "-o", "/tmp/pdoc-check",
         *PACKAGES],
        cwd=root, capture_output=True, text=True, timeout=120,
    )
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("range_", metavar="WHAT",
                    help="BASE..TIP or single COMMIT")
    ap.add_argument("--root", default=".",
                    help="repo root (with src/)")
    a = ap.parse_args()

    if ".." in a.range_:
        base, tip = a.range_.split("..")
        chain = subprocess.run(
            ["git", "-c", "safe.directory=*", "rev-list", "--reverse", f"{base}..{tip}"],
            cwd=a.root, capture_output=True, text=True,
            check=True).stdout.split()
    else:
        chain = [a.range_]

    bad = []
    for h in chain:
        # checkout the commit (worktree copy: use git -C archive to
        # avoid touching the user's checkout)
        workdir = "/tmp/pdoc-wt"
        subprocess.run(["rm", "-rf", workdir], check=False)
        subprocess.run(
            ["git", "-c", "safe.directory=*", "-C", a.root, "archive", h], check=True,
            stdout=subprocess.PIPE)
        import tarfile, io
        data = subprocess.run(
            ["git", "-c", "safe.directory=*", "-C", a.root, "archive", h], check=True,
            stdout=subprocess.PIPE).stdout
        with tarfile.open(fileobj=io.BytesIO(data)) as tf:
            tf.extractall(workdir, filter="fully_trusted")
        r = check_commit(h, workdir)
        subj = subprocess.run(
            ["git", "-c", "safe.directory=*", "-C", a.root, "log", "--format=%s", "-1", h],
            capture_output=True, text=True).stdout.strip()
        if r.returncode != 0:
            print(f"CRASH {h[:7]}: {subj[:50]}")
            print("  ", r.stderr.strip().splitlines()[-1][:100]
                  if r.stderr.strip() else "(no stderr)")
            bad.append(h)
        else:
            print(f"ok    {h[:7]} {subj[:50]}")
    print(f"{len(chain)} commits pdoc-checked: {len(bad)} crashes")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()