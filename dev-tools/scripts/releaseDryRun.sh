#!/bin/bash
# Dry run of the release process for the version in build.gradle: prepares the
# changelog (committed to a local "release-dry-run" branch, never pushed), builds
# an unsigned RC with buildAndPushRelease.py, then runs smokeTestRelease.py on it.
# Used by the Solr-Smoketest-* Jenkins jobs.
#
# Usage: dev-tools/scripts/releaseDryRun.sh [TMP_DIR]
# TMP_DIR must be outside the git checkout; it is deleted first.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

TMP_ROOT="${1:-${WORKSPACE:-$(pwd)}_smoke}"

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "Working tree has uncommitted changes" >&2
  exit 1
fi

pip3 install -r dev-tools/scripts/requirements.txt

[[ -e "$TMP_ROOT" ]] && chmod -R u+w "$TMP_ROOT"
rm -rf "$TMP_ROOT"
mkdir -p "$TMP_ROOT"

VERSION=$(cd dev-tools/scripts && python3 -c 'import scriptutil;print(scriptutil.find_current_version())')
echo "Dry run of release $VERSION in $TMP_ROOT"

ORIG_REF=$(git symbolic-ref -q --short HEAD || git rev-parse HEAD)
trap 'git checkout -q "$ORIG_REF"' EXIT

export GIT_AUTHOR_NAME=ci GIT_AUTHOR_EMAIL=ci@localhost GIT_COMMITTER_NAME=ci GIT_COMMITTER_EMAIL=ci@localhost
git checkout -B release-dry-run
python3 -u dev-tools/scripts/logchange.py prepare --version "$VERSION" --release-branch release-dry-run --commit

python3 -u dev-tools/scripts/buildAndPushRelease.py --dev-mode --push-local "$TMP_ROOT/dist" --rc-num 1

# Unit tests run in the regular CI jobs; here just check that the source release compiles.
export SMOKETEST_SRC_TEST_TASK=testClasses

python3 -u dev-tools/scripts/smokeTestRelease.py --not-signed --tmp-dir "$TMP_ROOT/smoke" \
  "file://$TMP_ROOT/dist/solr-$VERSION-RC1-rev-$(git rev-parse HEAD)"
