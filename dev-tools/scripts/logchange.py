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


def validate_unreleased_yamls(git_root, dry_run=False):
    """Run validate-changelog-yaml.py over changelog/unreleased/.

    Passes the folder path to the validator, which handles file discovery itself.
    Locates the validator relative to this script so it works regardless of cwd.
    If the validator cannot be found, logs a warning and returns True (proceed).
    Returns True if all files are valid (or nothing to validate), False otherwise.
    """
    unreleased_dir = git_root / "changelog" / "unreleased"
    if not unreleased_dir.exists():
        return True

    validator = Path(__file__).parent / "validate-changelog-yaml.py"
    if not validator.exists():
        print(f"  Warning: validator not found at {validator} — skipping YAML validation")
        return True

    print(f"\n[0] Validating changelog/unreleased/ with {validator.name}")
    if dry_run:
        print(f"  (dry-run) would run: {validator.name} {unreleased_dir}")
        return True

    result = subprocess.run(
        [sys.executable, str(validator), str(unreleased_dir)],
        cwd=git_root,
    )
    if result.returncode != 0:
        print("\nError: changelog YAML validation failed — please fix the issues and run again.",
              file=sys.stderr)
        return False
    return True


def ensure_unreleased_gitkeep(git_root, dry_run=False):
    """Ensure changelog/unreleased/.gitkeep exists and is staged.

    After a release the unreleased/ folder may be absent or untracked.
    Committing a .gitkeep guarantees contributors always find the folder.
    """
    gitkeep = git_root / "changelog" / "unreleased" / ".gitkeep"
    if not gitkeep.exists():
        print(f"  Creating {gitkeep.relative_to(git_root)}")
        if not dry_run:
            gitkeep.parent.mkdir(parents=True, exist_ok=True)
            gitkeep.touch()


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
    rc_suffix = f" RC{args.rc_number}" if args.rc_number > 1 else ""
    version_label = f"v{version}{rc_suffix}"

    version_dir = git_root / "changelog" / f"v{version}"
    unreleased_dir = git_root / "changelog" / "unreleased"
    version_summary = f"changelog/v{version}/version-summary.md"

    print(f"\n=== Changelog prepare for {version_label} on {release_branch} ===")
    if dry_run:
        print("    (dry-run — no changes will be made)\n")
    else:
        print()

    print(f"[1] Checking out {release_branch}")
    git(["checkout", release_branch], cwd=git_root, dry_run=dry_run)

    if not args.skip_validation:
        if not validate_unreleased_yamls(git_root, dry_run=dry_run):
            sys.exit(1)

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

    if do_commit:
        # Commit A: changelog/ YAML files only (version-summary.md is stale until generate runs)
        print(f"\n[3] Staging changelog/ entries for {version_label} (commit A)")
        ensure_unreleased_gitkeep(git_root, dry_run=dry_run)
        git(["add", "changelog"], cwd=git_root, dry_run=dry_run)
        # Unstage version-summary.md — it is stale until logchangeGenerate runs
        git(["restore", "--staged", version_summary], cwd=git_root, dry_run=dry_run, check=False)

        if not dry_run and not has_staged_changes(git_root):
            print("\nNothing staged — changelog entries are already up to date.")
        else:
            msg_a = f"Changelog entries for {version_label}"
            print(f"\n[4] Committing: {msg_a!r}")
            git(["commit", "-m", msg_a], cwd=git_root, dry_run=dry_run)

        # Commit B: regenerate, then stage CHANGELOG.md + version-summary.md
        print(f"\n[5] Running logchangeGenerate and stripping [unreleased] block")
        run_logchange_generate(gradle_cmd, git_root, dry_run=dry_run)

        print(f"\n[6] Staging CHANGELOG.md and version-summary.md (commit B)")
        git(["add", "CHANGELOG.md", version_summary], cwd=git_root, dry_run=dry_run)

        if not dry_run and not has_staged_changes(git_root):
            print("  Nothing staged — CHANGELOG.md already up to date.")
        else:
            msg_b = f"Regenerate CHANGELOG.md for {version_label}"
            print(f"\n[7] Committing: {msg_b!r}")
            git(["commit", "-m", msg_b], cwd=git_root, dry_run=dry_run)

        if not dry_run:
            print(f"\nDone. Last commit {short_sha(git_root)} on {release_branch}.")
            print(f"Push with:  git push {args.git_remote} {release_branch}")
    else:
        print(f"\n[3] Running logchangeGenerate and stripping [unreleased] block")
        run_logchange_generate(gradle_cmd, git_root, dry_run=dry_run)
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
    latest_lts_stable_branch = args.latest_lts_stable_branch
    release_date_str = args.release_date or date.today().isoformat()

    version_dir = git_root / "changelog" / f"v{version}"

    print(f"\n=== Changelog forward-port for v{version} ===")
    print(f"    Release date   : {release_date_str}")
    print(f"    release_branch : {release_branch}")
    print(f"    stable_branch  : {stable_branch}")
    print(f"    also targets   : main")
    if latest_lts_stable_branch:
        print(f"    lts also targets: {latest_lts_stable_branch}")
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

    version_summary = f"changelog/v{version}/version-summary.md"

    # Step 2: commit A — release-date.txt only (before generate)
    print(f"\n[2] Committing release-date.txt to {release_branch} (commit A)")
    git(["add", f"changelog/v{version}/release-date.txt"], cwd=git_root, dry_run=dry_run)

    if not dry_run and not has_staged_changes(git_root):
        print("  Nothing staged — release-date.txt was already up to date.")
    else:
        msg_a = f"Set release date {release_date_str} for v{version}"
        print(f"  Committing: {msg_a!r}")
        git(["commit", "-m", msg_a], cwd=git_root, dry_run=dry_run)

    # Step 3: commit B — regenerate, then stage CHANGELOG.md + version-summary.md
    print(f"\n[3] Running logchangeGenerate (with release date {release_date_str})")
    run_logchange_generate(args.gradle_cmd, git_root, dry_run=dry_run)

    print(f"\n[3b] Staging CHANGELOG.md and version-summary.md to {release_branch} (commit B)")
    git(["add", "CHANGELOG.md", version_summary], cwd=git_root, dry_run=dry_run)

    if not dry_run and not has_staged_changes(git_root):
        print("  Nothing staged — CHANGELOG.md already up to date.")
    else:
        msg_b = f"Regenerate CHANGELOG.md for v{version}"
        print(f"  Committing: {msg_b!r}")
        git(["commit", "-m", msg_b], cwd=git_root, dry_run=dry_run)

    # Steps 4+5: for each target branch, find commits on release_branch not yet on
    #            that target, cherry-pick them, then regenerate CHANGELOG.md fresh.
    #            CHANGELOG.md is never cherry-picked — it is always regenerated so
    #            each branch gets a correct full-history version (avoids cross-major
    #            conflicts).  version-summary.md is also excluded from cherry-picks
    #            for the same reason; it is regenerated alongside CHANGELOG.md.
    #            --cherry-pick with the symmetric-difference range (three dots) omits
    #            commits whose patch is already present on the target, making
    #            forward-port idempotent when re-run after an initial no-push run.
    targets = [stable_branch, "main"]
    if latest_lts_stable_branch:
        targets.append(latest_lts_stable_branch)

    for target in targets:
        print(f"\n[4] Finding changelog/ commits on {release_branch} not yet on {target}")
        if dry_run:
            print(f"  (dry-run) would run: git log --cherry-pick --right-only {target}...{release_branch} -- changelog/ :(exclude)changelog/*/version-summary.md")
            commits = []
        else:
            result = subprocess.run(
                ["git", "log", "--oneline", "--reverse",
                 "--cherry-pick", "--right-only",
                 f"{target}...{release_branch}",
                 "--", "changelog/", ":(exclude)changelog/*/version-summary.md"],
                cwd=git_root, capture_output=True, text=True, check=True,
            )
            commits = [line.split()[0] for line in result.stdout.strip().splitlines() if line]

        if not commits:
            print(f"  No changelog commits to forward-port — {target} is already up to date.")
        else:
            print(f"  Found {len(commits)} commit(s) to cherry-pick: {', '.join(commits)}")

        print(f"\n[5] Cherry-picking {len(commits)} commit(s) to {target}")
        git(["checkout", target], cwd=git_root, dry_run=dry_run)
        for sha in commits:
            # Commits that touch changelog/unreleased/ are deletions of files
            # that exist under different names on stable/main — use -X ours so
            # git keeps the target branch's own unreleased entries rather than
            # trying to delete them.  Commits that only add to the version
            # folder are clean additions and cherry-pick without strategy options.
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

        # Ensure unreleased/ folder exists for contributors after cherry-picks
        ensure_unreleased_gitkeep(git_root, dry_run=dry_run)
        git(["add", "changelog/unreleased/.gitkeep"], cwd=git_root, dry_run=dry_run)
        if not dry_run and has_staged_changes(git_root):
            git(["commit", "-m", f"Ensure changelog/unreleased/ folder exists on {target}"],
                cwd=git_root, dry_run=dry_run)

        # Regenerate CHANGELOG.md and version-summary.md fresh on this branch.
        print(f"\n  Regenerating CHANGELOG.md on {target}")
        run_logchange_generate(args.gradle_cmd, git_root, dry_run=dry_run)
        git(["add", "CHANGELOG.md", version_summary], cwd=git_root, dry_run=dry_run)
        if not dry_run and has_staged_changes(git_root):
            git(["commit", "-m", f"Regenerate CHANGELOG.md with v{version} entries on {target}"],
                cwd=git_root, dry_run=dry_run)

    # Step 6: push (optional)
    if do_push:
        remote = args.git_remote
        branches_to_push = [release_branch, stable_branch, "main"]
        if latest_lts_stable_branch:
            branches_to_push.append(latest_lts_stable_branch)
        print(f"\n[6] Pushing {', '.join(branches_to_push)} to {remote}")
        for branch in branches_to_push:
            git_push(branch, remote, cwd=git_root, dry_run=dry_run)
    else:
        print(f"\nCherry-picks done. Review, then re-run with --push to push all branches.")
        if not dry_run:
            print(f"  git log --oneline {stable_branch} -- changelog/")

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
    p.add_argument("--rc-number", type=int, default=1,
                   help="RC number (default: 1). RC2+ appends ' RC<n>' to commit messages.")
    p.add_argument("--skip-validation", action="store_true",
                   help="Skip pre-flight YAML validation of changelog/unreleased/ files")
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
    fp.add_argument("--latest-lts-stable-branch",
                    help="Also forward-port to this LTS stable branch, e.g. branch_9x (LTS releases only)")
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
