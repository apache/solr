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

# Forked and adapted from https://github.com/factorial-io/cherrypicker - MIT license
# Copyright (c) 2017 Shibin Das - @d34dman

function LOG() {
  local STATUS=$1
  local MESSAGE=$2
  echo "[$(date)] ${STATUS} ${MESSAGE}"
}

function usage() {
  cat << EOF
Usage: dev-tools/scripts/cherrypick.sh [<options>] <commit-hash> [<commit-hash>...]
 -b <branch> Sets the branch(es) to cherry-pick to, typically branch_Nx or branch_x_y
 -s          Skips precommit test. WARNING: Always run precommit for code- and doc changes
 -t          Run the full test suite during check, not only precommit
 -n          Skips git pull of target branch. Useful if you are without internet access
 -a          Enters automated mode. Aborts cherry-pick and exits on error
 -r <remote> Specify remote to push to. Defaults to whatever the branch is tracking.
 -p          Push to remote. Only done if both cherry-pick and tests succeeded
    WARNING: Never push changes to a remote branch before a thorough local test

Simple script for aiding in back-porting one or more (trivial) commits to other branches.
On merge conflict the script will run 'git mergetool'. See 'git mergetool --help'
for help on configuring your favourite merge tool. Check out Sublime Merge (smerge).

Example:
  # Backport two commits to both stable and release branches
  dev-tools/scripts/cherrypick.sh -b branch_9x -b branch_9_0 deadbeef0000 cafebabe1111
EOF
}

function yesno() {
  question=$1
  unset answer
  echo "$question"
  while [[ "$answer" != "y" ]] && [[ "$answer" != "n" ]]; do
    read -r answer
    if [[ "$answer" == "y" ]]; then
      true
    else
      false
    fi
  done
}

GIT_COMMAND=git
PRECOMMIT="true"
TESTARG="-x test"
TEST=
TESTS_PASSED=
PUSH=
REMOTE=
NOPULL=
AUTO_MODE=
unset BRANCHES

while getopts ":b:phstnar:" opt; do
  case ${opt} in
    b)
      BRANCHES+=("$OPTARG")
      ;;
    r)
      REMOTE=$OPTARG
      ;;
    p)
      PUSH=true
      ;;
    s)
      PRECOMMIT=
      ;;
    a)
      AUTO_MODE="true"
      ;;
    n)
      NOPULL="true"
      ;;
    t)
      TEST="true"
      TESTARG=""
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

COMMITS=( "$@" )

if [ ${#BRANCHES[@]} -eq 0 ]; then
  LOG INFO "Lacking -b option, must specify target branch. Supports multiple -b options too"
  usage
  exit
fi

if [ ${#COMMITS[@]} -eq 0 ]; then
  LOG ERROR "Please specify one or more commits"
  usage
  exit
fi

## Loop over branches
for BRANCH in "${BRANCHES[@]}"; do
  echo ""
  LOG "INFO" "============================================";
  LOG "INFO" "Git branch: $BRANCH"
  LOG "INFO" "============================================";
  echo ""
  LOG INFO "Checking out target branch $BRANCH"
  # shellcheck disable=SC2086
  $GIT_COMMAND checkout $BRANCH
  if [ $? -gt 0 ]; then
    LOG ERROR "Failed checking out branch $BRANCH"
    exit 2
  fi
  if [[ ! "$NOPULL" ]]; then
    # shellcheck disable=SC2086
    $GIT_COMMAND fetch $REMOTE
    # shellcheck disable=SC2086
    $GIT_COMMAND pull --ff-only $REMOTE
  else
    LOG INFO "Skipping git pull"
  fi

  # Loop over commits
  for i in "${COMMITS[@]}"
  do
    LOG "INFO" "::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::";
    LOG "INFO" "Processing commit          : $i"
    LOG "INFO" "::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::";

    $GIT_COMMAND cherry-pick "$i" -x
    if [ $? -eq 0 ]; then
      LOG "INFO" "Cherry pick of $i completed successfully."
      continue;
    fi
    LOG "ERROR" "Cherry pick encountered an error."
    if [[ "$AUTO_MODE" ]]; then
      LOG ERROR "Aborting cherry-pick and exiting. Please handle this manually"
      $GIT_COMMAND cherry-pick --abort
      exit 2
    fi
    if yesno "Do you want me to open mergetool? (y/n) "; then
      $GIT_COMMAND mergetool
    fi
    if yesno "Have you resolved the merge conflict? (y/n) "; then
      LOG INFO "Continuing..."
    else
      if yesno "Clean up by aborting the cherry-pick? (y/n) "; then
        $GIT_COMMAND cherry-pick --abort
      fi
      LOG INFO "Bye"
      exit 1
    fi
  done

  LOG INFO "All cherry-picks to branch $BRANCH succeeded"

  if [[ "$PRECOMMIT" ]] || [[ "$TEST" ]]; then
    LOG "INFO" "Testing the cherry-pick on $BRANCH by running 'gradlew check ${TESTARG}'"
    # shellcheck disable=SC2086
    ./gradlew check -q $TESTARG
    if [ $? -gt 0 ]; then
      LOG "WARN" "Tests failed. Please fix and push manually"
      exit 2
    fi
    TESTS_PASSED="true"
    LOG "INFO" "Tests passed on $BRANCH"
  else
    LOG "INFO" "Skipping tests"
  fi

  if [[ "$PUSH" ]]; then
    if [[ ! $TESTS_PASSED ]]; then
      LOG "WARN" "Cannot auto push if tests are disabled"
      exit 1
    fi
    if [[ -z "$REMOTE" ]]; then
      LOG "WARN" "Remote not specified, attempting to auto detect"
      REMOTE=$(git config --get "branch.$BRANCH.remote")
    fi
    LOG "INFO" "Pushing changes to $REMOTE/$BRANCH"
    # shellcheck disable=SC2086
    $GIT_COMMAND push $REMOTE $BRANCH
    if [ $? -gt 0 ]; then
      LOG "WARN" "PUSH to $REMOTE/$BRANCH failed, please clean up and proceed manually"
      exit 2
    fi
    LOG "INFO" "Pushed $BRANCH to remote. Cherry-pick complete."
  fi
done

LOG "INFO" "SUCCESS on branches ${BRANCHES[*]} for commits ${COMMITS[*]}"
