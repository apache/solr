#!/bin/bash
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

# Util script that helps with syncing logchange/unreleased folder across branches

function log() {
  local MESSAGE=$1
  echo "[$(date)] INFO ${MESSAGE}"
}

function error() {
  local MESSAGE=$1
  echo "[$(date)] ERROR ${MESSAGE}"
}

function debug() {
  local MESSAGE=$1
  if [[ $VERBOSE ]]; then
    echo "[$(date)] DEBUG ${MESSAGE}"
  fi
}

function git_exec() {
  unset SILENT
  if [ "$1" == "-s" ]; then
    SILENT=true
    shift
  fi
  local DESCRIPTION=$1
  shift
  local COMMAND=("$@")

  if [[ "$DRY_RUN" ]]; then
    if [[ ! $SILENT ]]; then
      log "[DRY RUN] Would execute: ${COMMAND[*]}"
    fi
  else
    debug "Executing: ${COMMAND[*]}"
    # shellcheck disable=SC2086
    "${COMMAND[@]}"
    if [ $? -gt 0 ]; then
      error "$DESCRIPTION failed"
      exit 2
    fi
  fi
}

function checkout() {
  local BRANCH=$1
  debug "Checking out branch $BRANCH"
  git_exec "Checking out branch $BRANCH" $GIT_COMMAND checkout $BRANCH
}

function usage() {
  cat << EOF
Usage: dev-tools/scripts/logchange-git-util.sh [<options>] <src-branch> <dst-branch> [<dst-branch>...]
 -r <remote> Specify remote, defaults to "origin"
 -o          Print the files that will/would be deleted
 -d          Delete common files from <dst> with git rm
 -c          Commit
 -p          Push to remote
 -f          Fetch from remote before starting
 -y          Dry-run mode (print commands without executing them)
 -v          Verbose

Script to help release managers sync logchange/unreleased files across branches
Assumes you are in git repo root of a solr checkout, and that all branches have
changelog/unreleased folders with various files in them.

The script processes each destination branch in turn, syncing unreleased changelog
entries from the source branch.

Example:
  # Sync from release branch branch_9_0 to stable branch branch_9x, fetch first
  dev-tools/scripts/logchange-git-util.sh -fdcp branch_9_0 branch_9x
  # Sync from branch_9_0 to multiple destinations
  dev-tools/scripts/logchange-git-util.sh -fdcp branch_9_0 branch_9x main
  # Dry run sync from branch_9_0 to unstable branch main
  dev-tools/scripts/logchange-git-util.sh -ydocp branch_9_0 main
EOF
}

GIT_COMMAND=git
unset FETCH
unset DELETE
unset COMMIT
unset PUSH
unset PRINT
unset VERBOSE
unset DRY_RUN
REMOTE=origin

while getopts ":r:fdcpvyoh" opt; do
  case ${opt} in
    r)
      REMOTE=$OPTARG
      ;;
    f)
      FETCH=true
      ;;
    d)
      DELETE=true
      ;;
    c)
      COMMIT=true
      ;;
    p)
      PUSH=true
      ;;
    o)
      PRINT=true
      ;;
    y)
      DRY_RUN=true
      ;;
    v)
      VERBOSE=true
      ;;
    h)
      usage
      exit 0
      ;;
   \?)
      echo "Unknown option $OPTARG"
      usage
      exit 1
   esac
done
shift $((OPTIND -1))

ARGS=( "$@" )

if [ ${#ARGS[@]} -lt 2 ]; then
  error "Mandatory positional args: <src-branch> <dst-branch> [<dst-branch>...]"
  usage
  exit 1
fi

SRC_BRANCH=$1
shift
DST_BRANCHES=( "$@" )

# Validate option combinations (must be done before processing)
if [[ "$COMMIT" ]] && ! [[ "$DELETE" ]]; then
  error "Cannot commit unless -d (delete) flag is specified"
  exit 1
fi

if [[ "$PUSH" ]] && ! [[ "$COMMIT" ]]; then
  error "Cannot push unless -c (commit) flag is specified"
  exit 1
fi

# Fetch once before processing all branches
if [[ "$FETCH" ]]; then
  log "Fetching..."
  git_exec "Fetching from remote $REMOTE" $GIT_COMMAND fetch $REMOTE
  git_exec "Pulling from remote $REMOTE" $GIT_COMMAND pull --ff-only $REMOTE
else
  debug "Skipping git fetch"
fi

# Process each destination branch
for DST_BRANCH in "${DST_BRANCHES[@]}"; do
  log ""
  log "Processing destination branch: $DST_BRANCH"
  log "================================"

  # Read git ls-tree output into temporary files for sorting and comparison
  SRC_FILES_TMP=$(mktemp)
  DST_FILES_TMP=$(mktemp)
  COMMON_FILES_TMP=$(mktemp)
  trap "rm -f $SRC_FILES_TMP $DST_FILES_TMP $COMMON_FILES_TMP" RETURN

  if [ ${#SRC_BRANCH} -eq 40 ]; then
    SRC_REF=$SRC_BRANCH
  else
    SRC_REF=$REMOTE/$SRC_BRANCH
  fi

  git ls-tree -r --name-only $SRC_REF -- changelog/unreleased/ | sort > "$SRC_FILES_TMP"
  git ls-tree -r --name-only $REMOTE/$DST_BRANCH -- changelog/unreleased/ | sort > "$DST_FILES_TMP"

  # Find common files between the two sorted lists
  comm -12 "$SRC_FILES_TMP" "$DST_FILES_TMP" > "$COMMON_FILES_TMP"

  src_file_count=$(wc -l < "$SRC_FILES_TMP")
  dst_file_count=$(wc -l < "$DST_FILES_TMP")
  file_count=$(wc -l < "$COMMON_FILES_TMP")
  debug "Found $file_count common files"

  log "Checking out destination branch $DST_BRANCH"
  checkout $DST_BRANCH

  # Print files that would be deleted
  if [[ "$PRINT" ]]; then
    log "Files that would be deleted from $DST_BRANCH:"
    cat "$COMMON_FILES_TMP"
  fi

  if [[ "$DELETE" ]]; then
    log "Deleting common files from $DST_BRANCH"
    # Delete files using git rm
    while IFS= read -r file; do
      debug "Removing $file"
      git_exec -s "Removing $file" $GIT_COMMAND rm "$file"
    done < "$COMMON_FILES_TMP"
    if [[ "$DRY_RUN" ]]; then
      log "[DRY RUN] Would remove $file_count files of the $dst_file_count unreleased files in $DST_BRANCH"
    else
      log "Successfully removed $file_count files from $DST_BRANCH"
    fi
  fi

  if [[ "$COMMIT" ]]; then
    log "Committing changes to $DST_BRANCH"
    git_exec "Committing changes" $GIT_COMMAND commit -m "Sync changelog/unreleased from $SRC_BRANCH to $DST_BRANCH"
  fi

  if [[ "$PUSH" ]]; then
    log "Pushing changes to $REMOTE/$DST_BRANCH"
    git_exec "Pushing to $REMOTE/$DST_BRANCH" $GIT_COMMAND push $REMOTE $DST_BRANCH
  fi
done

# Final summary
if [[ "$DRY_RUN" ]]; then
  log ""
  log "[DRY RUN] Completed successfully (no changes made)"
else
  log ""
  log "DONE - Processed ${#DST_BRANCHES[@]} destination branch(es)"
fi
