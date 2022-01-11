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
  echo "Script simplifying the process of back-porting a single commit to other branches"
  echo ""
  echo "Usage: dev-tools/scripts/cherrypick.sh -b <target-branch> [-p <remote>] [-m <merge-tool>] [-t] <commit-hash> [<commit-hash>...]"
  echo "   -b <target-branch> sets the branch to cherry-pick to, typically branch_Nx or branch_x_y"
  echo "   -s skips precommit test. WARNING: Always run precommit for non-trivial backports"
  echo "   -n skips git pull of target branch. Useful if you are without internet access."
  echo "   -t runs the full test suite during check, not only precommit"
  echo "   -a enters automated mode, where failure to cherry-pick will not prompt but abort merge and exit with error"
  echo "   -r <remote> Specify remote if other than 'origin'"
  echo "   -p will push to given remote, or 'origin' if not specified, only if both cherry-pick and tests succeeds."
  echo "      WARNING: Never push changes to a remote branch before a thorough test."
  echo "   <commit-hash> are the commit hash(es) (on main branch) to cherry-pick. May be several in a sequence"
  echo ""
  echo "   On merge conflict the script will run 'git mergetool', unless -a is given. See 'git mergetool --help' for"
  echo "   help on configuring your favourite merge tool. Check out Sublime Merge (smerge)"
  echo ""
}

function yesno() {
  question=$1
  unset answer
  echo $question
  while [[ "$answer" != "y" ]] && [[ "$answer" != "n" ]]; do
    read answer
    if [[ "$answer" == "y" ]]; then
      true
    else
      false
    fi
  done
}

GIT_COMMAND=git
PRECOMMIT="true"
TESTARG=" -x test"
TEST=
TESTS_PASSED=
PUSH=
REMOTE=origin
NOPULL=
AUTO_MODE=

while getopts ":b:phstnar:" opt; do
  case ${opt} in
    b)
      BRANCH=$OPTARG
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

if [ -z "$BRANCH" ]; then
  LOG INFO "Lacking -b option, must specify target branch"
  usage
  exit
fi

if [ ${#COMMITS[@]} -eq 0 ]; then
  LOG ERROR "Please specify one or more commits"
  usage
  exit
fi

LOG INFO "Checking out target branch $BRANCH"
$GIT_COMMAND checkout "$BRANCH"
if [[ ! "$NOPULL" ]]; then
  $GIT_COMMAND pull --ff-only
else
  LOG INFO "Skipping git pull"
fi

## main processing loop
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

LOG INFO "All cherry-pick succeeded"

if [[ "$PRECOMMIT" ]] || [[ "$TEST" ]]; then
  LOG "INFO" "Testing the cherry-pick by running 'gradlew check${TESTARG}'"
  ./gradlew check${TESTARG}
  if [ $? -gt 0 ]; then
    LOG "WARN" "Tests failed. Please fix and push manually"
    exit 2
  fi
  TESTS_PASSED="true"
  LOG "INFO" "Tests passed"
else
  LOG "INFO" "Skipping tests"
fi

if [[ "$PUSH" ]]; then
  if [[ ! $TESTS_PASSED ]]; then
    LOG "WARN" "Cannot auto push if tests are disabled" 
    exit 1
  fi
  LOG "INFO" "Pushing changes to $REMOTE/$BRANCH"
  $GIT_COMMAND push "$REMOTE"
  if [ $? -gt 0 ]; then
    LOG "WARN" "PUSH to $REMOTE/$BRANCH failed, please clean up an proceed manually"
    exit 2
  fi
  LOG "INFO" "Pushed to remote. Cherry-pick complete."
fi

