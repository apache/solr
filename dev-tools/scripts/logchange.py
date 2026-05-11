#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Changelog release helper for Apache Solr release managers.

Normally invoked by the Release Wizard; see dev-docs/changelog.adoc for details.
"""

import argparse
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_git_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    )
    return Path(result.stdout.strip())


def run(cmd, *, cwd, dry_run=False, check=True, capture=False):
    """Print and optionally execute a shell command."""
    print(f"  $ {' '.join(str(c) for c in cmd)}")
    if dry_run:
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    return subprocess.run(
        cmd, cwd=cwd, capture_output=capture, text=True, check=check,
    )


def git(args, *, cwd, dry_run=False, check=True, capture=False):
    return run(["git"] + args, cwd=cwd, dry_run=dry_run, check=check, capture=capture)


def git_push(branch, remote, *, cwd, dry_run=False):
    git(["checkout", branch], cwd=cwd, dry_run=dry_run)
    git(["push", remote, branch], cwd=cwd, dry_run=dry_run)


def short_sha(git_root):
    r = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=git_root, capture_output=True, text=True, check=True,
    )
    return r.stdout.strip()


def has_staged_changes(git_root):
    """Return True if there are staged changes in the index."""
    r = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=git_root, capture_output=True, text=True,
    )
    return bool(r.stdout.strip())


def commit_touches_unreleased(sha, git_root):
    """Return True if the given commit modifies any file under changelog/unreleased/."""
    r = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "-r", "--name-only", sha],
        cwd=git_root, capture_output=True, text=True, check=True,
    )
    return any(f.startswith("changelog/unreleased/") for f in r.stdout.splitlines())


def strip_unreleased_block(changelog_path: Path, dry_run=False):
    """Remove the [unreleased] block that logchangeGenerate emits.

    CHANGELOG.md uses logchange's setext-style headings, e.g.:

        [unreleased]
        ------------

        ### Added ...
        ...content...

        [10.1.0]
        --------

    We strip from the ``[unreleased]`` line up to (but not including) the next
    versioned heading, identified by a ``[`` at the start of a line that is not
    ``[unreleased]``.
    """
    if not changelog_path.exists():
        print("  (CHANGELOG.md not found — skipping strip)")
        return
    text = changelog_path.read_text(encoding="utf-8")
    cleaned = re.sub(
        r'^\[unreleased\]\n(?:.*\n)*?(?=^\[(?!unreleased\]))',
        '',
        text,
        flags=re.MULTILINE,
    )
    if cleaned == text:
        print("  (no [unreleased] block found in CHANGELOG.md — nothing stripped)")
        return
    print("  Stripped [unreleased] block from CHANGELOG.md")
    if not dry_run:
        changelog_path.write_text(cleaned, encoding="utf-8")


def run_logchange_generate(gradle_cmd, git_root, dry_run=False):
    run([gradle_cmd, "logchangeGenerate"], cwd=git_root, dry_run=dry_run)
    strip_unreleased_block(git_root / "CHANGELOG.md", dry_run=dry_run)
    # logchangeGenerate may create/touch changelog/unreleased/version-summary.md;
    # remove it so it doesn't clutter the working tree.
    summary = git_root / "changelog" / "unreleased" / "version-summary.md"
    if summary.exists():
        print(f"  Removing {summary.relative_to(git_root)}")
        if not dry_run:
            summary.unlink()


# ---------------------------------------------------------------------------
# prepare
# ---------------------------------------------------------------------------

def cmd_prepare(args, git_root):
    dry_run = args.dry_run
    do_commit = args.commit
    version = args.version
    release_branch = args.release_branch
    gradle_cmd = args.gradle_cmd

    version_dir = git_root / "changelog" / f"v{version}"
    unreleased_dir = git_root / "changelog" / "unreleased"

    print(f"\n=== Changelog prepare for v{version} on {release_branch} ===")
    if dry_run:
        print("    (dry-run — no changes will be made)\n")
    else:
        print()

    print(f"[1] Checking out {release_branch}")
    git(["checkout", release_branch], cwd=git_root, dry_run=dry_run)

    # RC1 vs RC2+ detection is done on the filesystem after checkout.
    # In dry-run mode the checkout is skipped, so warn the user the detection
    # may reflect the current branch rather than the release branch.
    if dry_run and not version_dir.exists():
        print(f"  (dry-run: branch not actually checked out; RC path detection "
              f"is based on current working tree and may be inaccurate)")

    if not version_dir.exists():
        # ------------------------------------------------------------------ #
        # RC1 path: version folder does not yet exist                         #
        # ------------------------------------------------------------------ #
        print(f"\n[2] RC1 path — running logchangeRelease --releaseDate none --versionToRelease {version}")
        run([gradle_cmd, "logchangeRelease", "--releaseDate", "none", "--versionToRelease", version],
            cwd=git_root, dry_run=dry_run)
    else:
        # ------------------------------------------------------------------ #
        # RC2+ path: version folder already exists, move only new entries     #
        # ------------------------------------------------------------------ #
        print(f"\n[2] RC2+ path — copying new unreleased entries to {version_dir.name}/")

        if not unreleased_dir.exists():
            print("  changelog/unreleased/ does not exist — nothing to move.")
        else:
            existing = {f.name for f in version_dir.iterdir() if f.is_file()}
            new_files = [
                f for f in unreleased_dir.iterdir()
                if f.is_file() and f.name not in existing
            ]

            if not new_files:
                print("  No new entries in changelog/unreleased/ — nothing to move.")
            else:
                for src in sorted(new_files):
                    dst = version_dir / src.name
                    print(f"  Moving {src.name} → {version_dir.name}/")
                    if not dry_run:
                        shutil.copy2(src, dst)
                        src.unlink()

    print(f"\n[3] Running logchangeGenerate and stripping [unreleased] block")
    run_logchange_generate(gradle_cmd, git_root, dry_run=dry_run)

    if do_commit:
        print(f"\n[4] Staging changelog/ and CHANGELOG.md")
        git(["add", "changelog", "CHANGELOG.md"], cwd=git_root, dry_run=dry_run)

        if not dry_run and not has_staged_changes(git_root):
            print("\nNothing staged — changelog is already up to date.")
            return

        msg = f"Changelog prepare for v{version} (no release date, RC prep)"
        print(f"\n[5] Committing: {msg!r}")
        git(["commit", "-m", msg], cwd=git_root, dry_run=dry_run)

        if not dry_run:
            print(f"\nDone. Commit {short_sha(git_root)} on {release_branch}.")
            print(f"Push with:  git push {args.git_remote} {release_branch}")
    else:
        print(f"\nReview the changes above, then run with --commit to stage and commit.")
        if not dry_run:
            print(f"  git diff changelog/ CHANGELOG.md")

    if dry_run:
        print("\nDry-run complete — no changes made.")


# ---------------------------------------------------------------------------
# forward-port
# ---------------------------------------------------------------------------

def cmd_forward_port(args, git_root):
    dry_run = args.dry_run
    do_push = args.push
    version = args.version
    release_branch = args.release_branch
    stable_branch = args.stable_branch
    release_date_str = args.release_date or date.today().isoformat()

    version_dir = git_root / "changelog" / f"v{version}"

    print(f"\n=== Changelog forward-port for v{version} ===")
    print(f"    Release date   : {release_date_str}")
    print(f"    release_branch : {release_branch}")
    print(f"    stable_branch  : {stable_branch}")
    print(f"    also targets   : main")
    if dry_run:
        print("    (dry-run — no changes will be made)")
    elif not do_push:
        print("    (push disabled — run with --push when ready)")
    print()

    # Step 1: checkout release branch and write release-date.txt
    print(f"[1] Checking out {release_branch} and writing release-date.txt")
    git(["checkout", release_branch], cwd=git_root, dry_run=dry_run)

    if not dry_run and not version_dir.exists():
        print(f"Error: version folder {version_dir} does not exist. "
              f"Run 'prepare' first.", file=sys.stderr)
        sys.exit(1)

    date_file = version_dir / "release-date.txt"
    print(f"  Writing {date_file.relative_to(git_root)}: {release_date_str}")
    if not dry_run:
        date_file.write_text(release_date_str + "\n", encoding="utf-8")

    # Step 2: regenerate CHANGELOG.md (now with the release date)
    print(f"\n[2] Running logchangeGenerate (with release date {release_date_str})")
    run_logchange_generate(args.gradle_cmd, git_root, dry_run=dry_run)

    # Step 3: stage and commit
    print(f"\n[3] Staging and committing to {release_branch}")
    git(["add", "changelog", "CHANGELOG.md"], cwd=git_root, dry_run=dry_run)

    if not dry_run and not has_staged_changes(git_root):
        print("  Nothing staged — release-date.txt and CHANGELOG.md were already up to date.")
    else:
        msg = f"Set release date {release_date_str} and regenerate CHANGELOG.md for v{version}"
        print(f"  Committing: {msg!r}")
        git(["commit", "-m", msg], cwd=git_root, dry_run=dry_run)

    # Step 4: find commits on release_branch not yet on stable_branch that
    #         touch changelog/ or CHANGELOG.md.  --cherry-pick with the
    #         symmetric-difference range (three dots) omits commits whose patch
    #         is already present on the target, making forward-port idempotent
    #         when re-run after an initial no-push review run.
    print(f"\n[4] Finding changelog commits on {release_branch} not yet on {stable_branch}")
    result = subprocess.run(
        ["git", "log", "--oneline", "--reverse",
         "--cherry-pick", "--right-only",
         f"{stable_branch}...{release_branch}",
         "--", "changelog/", "CHANGELOG.md"],
        cwd=git_root, capture_output=True, text=True, check=True,
    )
    commits = [line.split()[0] for line in result.stdout.strip().splitlines() if line]

    if not commits:
        print(f"  No changelog commits to forward-port — {stable_branch} is already up to date.")
    else:
        print(f"  Found {len(commits)} commit(s) to cherry-pick: {', '.join(commits)}")

        for target in [stable_branch, "main"]:
            print(f"\n[5] Cherry-picking {len(commits)} commit(s) to {target}")
            git(["checkout", target], cwd=git_root, dry_run=dry_run)
            for sha in commits:
                # Commits that touch changelog/unreleased/ are deletions of files
                # that exist under different names on stable/main — use -X ours so
                # git keeps the target branch's own unreleased entries rather than
                # trying to delete them.  Commits that only add to the version
                # folder or update CHANGELOG.md are clean additions; cherry-pick
                # them plainly so real conflicts are not silently discarded.
                if commit_touches_unreleased(sha, git_root):
                    cp_args = ["cherry-pick", "-X", "ours", "-X", "no-renames", sha]
                else:
                    cp_args = ["cherry-pick", sha]
                try:
                    git(cp_args, cwd=git_root, dry_run=dry_run)
                except subprocess.CalledProcessError:
                    print(f"\nError: cherry-pick of {sha} failed on {target}.",
                          file=sys.stderr)
                    print("  Resolve the conflict, then run: git cherry-pick --continue",
                          file=sys.stderr)
                    print("  Or abort with:                  git cherry-pick --abort",
                          file=sys.stderr)
                    sys.exit(1)

    # Step 6: push (optional)
    if do_push:
        remote = args.git_remote
        print(f"\n[6] Pushing {release_branch}, {stable_branch}, main to {remote}")
        for branch in [release_branch, stable_branch, "main"]:
            git_push(branch, remote, cwd=git_root, dry_run=dry_run)
    else:
        print(f"\nCherry-picks done. Review, then re-run with --push to push all branches.")
        if not dry_run:
            print(f"  git log --oneline {stable_branch} -- changelog/ CHANGELOG.md")

    if dry_run:
        print("\nDry-run complete — no changes made.")
    elif do_push:
        print("\nDone.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    fmt = lambda prog: argparse.RawDescriptionHelpFormatter(prog, width=120)
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="For options on each subcommand run: logchange.py <subcommand> --help",
        formatter_class=fmt,
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--version", required=True,
                        help="Release version, e.g. 10.1.0")
    common.add_argument("--release-branch", required=True,
                        help="Release branch, e.g. branch_10_1")
    common.add_argument("--gradle-cmd", default="./gradlew",
                        help="Gradle wrapper command (default: ./gradlew)")
    common.add_argument("--git-remote", default="origin",
                        help="Git remote name to push to (default: origin)")
    common.add_argument("--dry-run", action="store_true",
                        help="Print actions without executing or modifying anything")

    # prepare
    p = sub.add_parser("prepare", parents=[common],
                       help="Prepare changelog for an RC (RC1 or RC2+)",
                       description="Prepare the changelog for a release candidate.\n"
                                   "RC1: runs 'gradlew logchangeRelease' to move all unreleased entries to the version folder.\n"
                                   "RC2+: copies only new unreleased entries into the existing version folder.\n"
                                   "In both cases CHANGELOG.md is regenerated. By default changes are left uncommitted for review.",
                       formatter_class=fmt)
    p.add_argument("--commit", action="store_true",
                   help="Stage and commit the result (default: leave uncommitted for review)")
    p.set_defaults(func=cmd_prepare)

    # forward-port
    fp = sub.add_parser("forward-port", parents=[common],
                        help="Set release date and forward-port changelog post-vote",
                        description="Run once after a successful vote.\n"
                                    "Writes the release date to the version folder and regenerates CHANGELOG.md.\n"
                                    "Then cherry-picks all changelog commits from the release branch to the stable branch and main.\n"
                                    "By default nothing is pushed; review first and pass --push when satisfied.",
                        formatter_class=fmt)
    fp.add_argument("--stable-branch", required=True,
                    help="Stable branch, e.g. branch_10x")
    fp.add_argument("--release-date",
                    help="Release date YYYY-MM-DD (default: today)")
    fp.add_argument("--push", action="store_true",
                    help="Push all branches after cherry-picking (default: leave for review)")
    fp.set_defaults(func=cmd_forward_port)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if hasattr(args, 'release_date') and args.release_date:
        try:
            date.fromisoformat(args.release_date)
        except ValueError:
            print(f"Error: --release-date must be YYYY-MM-DD, got: {args.release_date!r}",
                  file=sys.stderr)
            sys.exit(1)

    try:
        git_root = find_git_root()
    except subprocess.CalledProcessError:
        print("Error: must be run from within the Solr git repository.", file=sys.stderr)
        sys.exit(1)

    try:
        args.func(args, git_root)
    except subprocess.CalledProcessError as e:
        print(f"\nError: command failed with exit code {e.returncode}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()
