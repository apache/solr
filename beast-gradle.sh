#!/usr/bin/env bash
# Gradle parallel-load beast harness (worktree-isolated) for the solr-ref fork.
#
# Each concurrent worker runs in its OWN git worktree (separate build/ dir) so there are no
# shared-build races (the naive single-tree approach fails with "Couldn't read file content:
# .../build/resources/test/..."). Workers run the SAME test for -i iterations (-Ptests.iters);
# pass/fail is the gradle EXIT CODE. On failure a worker keeps its worktree's test-results.
#
# Usage: beast-gradle.sh -m <module> -t <FQN test> [-p parallel] [-i iters] [-s seed] [-N]
#   -m gradle module (default :solr:core)   -t FQN test (REQUIRED)
#   -p concurrent workers (default 4)        -i tests.iters per worker (default 10)
#   -s fixed tests.seed (optional)           -N -Ptests.nightly=true
#   -k keep worktrees after run (default: reuse pool, never auto-delete)
#
# Total executions = p*i. Exit 0 = all workers green.

set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WTBASE="/tmp/beast-wt"
MODULE=":solr:core"; TEST=""; PAR=4; ITERS=10; SEED=""; NIGHTLY=""; PATCH=""
while getopts ":m:t:p:i:s:ND:k" opt; do case "$opt" in
  m) MODULE="$OPTARG";; t) TEST="$OPTARG";; p) PAR="$OPTARG";; i) ITERS="$OPTARG";;
  s) SEED="$OPTARG";; N) NIGHTLY="-Ptests.nightly=true";; D) PATCH="$OPTARG";; k) :;; *) echo bad; exit 2;; esac; done
[ -z "$TEST" ] && { echo "ERROR: -t <FQN test> required"; exit 2; }
# -D <patch>: apply an uncommitted working-tree diff into each worktree after checkout, so beast can
# exercise un-committed changes (worktrees are checked out at committed HEAD). Resolve to absolute path.
if [ -n "$PATCH" ]; then
  case "$PATCH" in /*) :;; *) PATCH="$(cd "$(dirname "$PATCH")" && pwd)/$(basename "$PATCH")";; esac
  [ -f "$PATCH" ] || { echo "ERROR: -D patch file not found: $PATCH"; exit 2; }
fi

HEADSHA="$(git -C "$REPO" rev-parse HEAD)"
MODPATH="${MODULE#:}"; MODPATH="${MODPATH//://}"   # :solr:core -> solr/core
SAFE="$(echo "$TEST" | tr '/:.' '___')"
RESULTS="/tmp/beast/$SAFE"; rm -rf "$RESULTS"; mkdir -p "$RESULTS"

echo "Beasting $TEST  module=$MODULE  parallel=$PAR  iters=$ITERS  seed=${SEED:-random}  nightly=${NIGHTLY:-no}  HEAD=$(git -C "$REPO" rev-parse --short HEAD)  patch=${PATCH:-none}"

# --- ensure a pool of P worktrees, each reset (detached) to current HEAD, optionally patched, compiled ---
ensure_worktree() {
  local w="$1"; local wt="$WTBASE/w$w"
  if [ ! -d "$wt/.git" ] && [ ! -f "$wt/.git" ]; then
    git -C "$REPO" worktree add --detach "$wt" "$HEADSHA" >/dev/null 2>&1
  fi
  # Hard-reset to HEAD (discards any patch applied by a prior run); build/ is gitignored and survives.
  git -C "$wt" reset --hard "$HEADSHA" >/dev/null 2>&1
  cp "$REPO/.java-version" "$wt/.java-version" 2>/dev/null
  if [ -n "$PATCH" ]; then
    git -C "$wt" apply "$PATCH" || { echo "WORKER $w: PATCH APPLY FAILED" >&2; return 1; }
  fi
}
echo "Preparing $PAR worktrees + compiling test classes (one-time per pool)..."
prep_pids=()
for w in $(seq 1 "$PAR"); do
  ( ensure_worktree "$w"; cd "$WTBASE/w$w" && ./gradlew --no-daemon -Dorg.gradle.java.home=/usr/lib/jvm/java-11-openjdk-amd64 "$MODULE:testClasses" -q >"$RESULTS/prep-w$w.log" 2>&1 ) &
  prep_pids+=($!)
done
for pid in "${prep_pids[@]}"; do wait "$pid"; done
echo "Worktrees ready."

run_worker() {
  local w="$1"; local wt="$WTBASE/w$w"
  local seedArg=""; [ -n "$SEED" ] && seedArg="-Ptests.seed=$SEED"
  ( cd "$wt" && ./gradlew --no-daemon -Dorg.gradle.java.home=/usr/lib/jvm/java-11-openjdk-amd64 "$MODULE:test" --tests "$TEST" \
      -Ptests.iters="$ITERS" $seedArg $NIGHTLY ) >"$RESULTS/w$w.log" 2>&1
  local rc=$?
  local xml="$wt/$MODPATH/build/test-results/test"
  if [ $rc -ne 0 ]; then
    echo "WORKER $w: FAIL (rc=$rc)"
    mkdir -p "$RESULTS/w$w-fail"
    cp -r "$xml/outputs" "$RESULTS/w$w-fail/outputs" 2>/dev/null
    cp "$xml/"TEST-*.xml "$RESULTS/w$w-fail/" 2>/dev/null
    grep -hoE "Can not find doc [0-9]+|Expect new leader|differs from control|AssertionError[^\"]*|cloud score.*|[A-Za-z.]+Exception: [^\"]{0,80}" "$xml/"TEST-*.xml 2>/dev/null | sort -u | head -6
  else
    echo "WORKER $w: ok"
  fi
  return $rc
}

pids=()
for w in $(seq 1 "$PAR"); do run_worker "$w" & pids+=($!); done
fails=0
for pid in "${pids[@]}"; do wait "$pid" || fails=$((fails+1)); done
echo "=== BEAST DONE: $fails/$PAR workers failed (each $ITERS iters; total $((PAR*ITERS)) execs). Results: $RESULTS ==="
[ $fails -eq 0 ] && exit 0 || exit 1
