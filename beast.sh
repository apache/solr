#!/usr/bin/env bash

#set -x #echo on
#echo "args:$@"

# The best Lucene / Solr beasting script in the world. TM. 4.3.0
#
# This script will fire off N independent test runs for a class, X of them in parallel.
#
# A results.log file is written to the {results-dir}. A non zero exit status indicates a failed run.
# If the test was executed properly there will always be one line entry for each run in the results.log file, success or failure.
#
# Logs for each run are independently written under the {results-dir}/1 directory.
#
# Why is it so great? It outsources all of the heavy lifting and runs each iteration independently, ensuring complete test isolation. 
#
# Required: 
#
# 1. sudo apt-get install parallel or equiv for other package managers
#
# Example: cd /workspace/lucene-solr/lucene
# Example: beast.sh TestClass
# Example: beast.sh -c TestClass
# Example: beast.sh -c -d /workspace/lucene-solr/lucene -i 12 -p 4 TestClass
#
# Cmd Param Help
# ---------------
# -d the location of the lucene-solr repo checkout
# -t temporary use directory
# -r the directory to write test results to
# -c use to clean and compile project before running tests - you want to do this the first time to avoid build races.
# -x run in headless mode, no progress display, no guake, better for scripts, jenkins, etc
# -i how many times to run the test
# -p how many parallel executions of the test to launch
# One argument: the simple name of the test class to run or a glob pattern
#
# Optional:
# 
# If you install guake, a result monitoring tab is opened for you in interactive mode (-i).

# Trouble Shooting:
# * Hangs in compile on resolve: see LUCENE-6743. Make sure you are using Ivy 2.4.0 or greater.
#   Fix hang workaround: find ~/.ivy2 -name "*.lck" -type f -exec rm {} \;

# NOTE: Ivy 2.4.0 is required. How to upgrade if your version is older:
#
# In ~/build.properties add:
# ivy.bootstrap.version=2.4.0
# ivy_checksum_sha1=5abe4c24bbe992a9ac07ca563d5bd3e8d569e9ed
# ivy.lock-strategy=artifact-lock-nio

# LICENSE
#
# Copyright 2016 Mark Miller
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.  

if [ "$(uname -s)" = "Darwin" ]; then
  command -v parallel >/dev/null 2>&1 || { echo "You must install parallel - try getting homebrew and then:  brew install parallel"; exit 1; }
fi  

if [ "$(uname -s)" = "Linux" ]; then
  command -v parallel >/dev/null 2>&1 || { echo "You must install parallel - try: apt-get -y install parallel"; exit 1; }
fi

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Initialize our own variables:
baseDir="$PWD"
tmpDir=$(dirname $(mktemp tmp.XXXXXXXXXX -ut))/beast-tmp
resultsDir=""
headless="n"
clean="n"
compile="y"
iters=5
parr=2

while getopts ":cs:xd:r:ht:i:p:" opt; do
    case "$opt" in
    c)
        clean="y"
        ;;
    s)
        compile="n"
        ;;
    d)
        baseDir=$OPTARG
        ;;
    h)
        echo "usage: beast.sh -c -d /workspace/lucene-solr/lucene -i 12 -p 4 TestClass "
        exit 1
        ;;
    r)  resultsDir=$OPTARG
        ;;
    t)  tmpDir=$OPTARG
        ;;
    i)  iters=$OPTARG
        ;;
    x)  headless="y"
        ;;
    p)  parr=$OPTARG
        ;;
    *)  echo "Invalid arg"
        ;;
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift


testClass=$1

if [ -z ${resultsDir} ] ; then
  resultsDir="${baseDir}/beast-results/$testClass"
fi

trap 'jobs -p | xargs kill > /dev/null 2>&1' EXIT

# some pretty progress for the user as compile can take > 1 min
progress()
{
	if [ "$headless" = "n" ]; then
	    local pid=$1
	    local outfile=$2
	    local delay=2.00
	    local progressString='|/-\'
	    local startTime=$(date +%s)
	    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
	        local temp=${progressString#?}
	        local endTime=$(date +%s)
	        local nowtime=$(($endTime-$startTime))
	        local outsize=0
	        if [ ! -z "$outfile" ]; then
	          outsize=$(wc -c <"$outfile")
	        fi
	        printf " [%c]  " "$progressString"
	        printf "%05d" $nowtime
	        printf "sec "
	        if [ ! -z "$outfile" ]; then
	        	printf "%08d" $outsize
	        	printf "bytes"
	        fi
	        local progressString=$temp${progressString%"$temp"}
	        sleep $delay
	        printf "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b"
            if [ ! -z "$outfile" ]; then
              printf "\b\b\b\b\b\b\b\b\b\b\b\b\b"
            fi
	    done
	    printf "    \b\b\b\b"
	fi
}

run_guake()
{ 
  # may not have guake intalled, but that is okay, we dump output to /dev/null
  sleep 2; guake -n quake -e "tail --pid=$$ -f $1 | cut -c1-50; exit 0" guake -r "$2" >/dev/null 2>&1
}

open_guake_tabs()
{
  # may not have guake intalled, but that is okay, we dump output to /dev/null
  sleep 2; guake -n quake -e "tail --pid=$$ -f ${baseDir}/$resultsDir/results.log | cut -c1-50; exit 0" guake -r "beast-results-$testClass" >/dev/null 2>&1
  sleep 2; guake -n quake -e "tail --pid=$$ -f ${baseDir}/$resultsDir/1/1/stderr; exit 0" guake -r "beast-stderr-1-$testClass" >/dev/null 2>&1
  sleep 2; guake -n quake -e "tail --pid=$$ -f ${baseDir}/$resultsDir/1/1/stdout; exit 0" guake -r "beast-stdout-1-$testClass" >/dev/null 2>&1
}

cd $baseDir

rm -r -f $resultsDir

mkdir -p $resultsDir
mkdir -p $tmpDir/parallel-tmp

echo -e "\n-->Test Beasting Script\n"
echo "Project: $baseDir"
echo "Clean and compile: $cleanAndCompile Headless: $headless"
if [ ! -z $testClass ]; then
  echo "Test class: $testClass Running $iters iterations, $parr at a time."
  echo "Results Dir: $resultsDir"
fi

cd ..

# first we look for the test

if [ -z "${testClass}" ] 
then
  echo "No test class was specified"
  exit 1
fi


echo "Looking for test: ${testClass}"
file=$(find "${baseDir}" -name "${testClass}.java")

if [ -z "${file}" ]; then
  echo "Could not find test class ${testClass} in ${baseDir}"
  exit 1
fi

directory=$(dirname "${file}" || { echo "Failed getting parent directory for ${file}"; exit 1; }) || { exit 1; }

module=""
while true; do 
  if [ -f "${directory}/ivy.xml" ]; then
    echo "module dir: ${directory}"
    module=${directory}
    break
  fi
  lastDirectory="${directory}"
  directory=$(dirname ${directory}  || { echo "Failed getting parent directory for ${file}"; exit 1; }) || { exit 1; }
  
  if [ "${lastDirectory}" = "${directory}" ]; then
    echo "Could not find module for ${test}"
    exit 1
  fi

done

if [ "${clean}" = "y" ]; then
  echo -e "\nTop level clean..."

  if [ "${compile}" = "n" ]; then
    echo -e "\nSkip compile is ignored on clean"
    compile="y"
  fi

  ant clean > $tmpDir/ant-clean-output.txt 2>&1 
  if [ $? -ne 0 ]; then
    cat $tmpDir/ant-clean-output.txt	
    echo "Top level clean failed"
    exit 1
  fi

  echo "Top level clean finished successfully"
fi

if [ "${compile}" = "y" ]; then
  echo -e "\nCompile..."

  if [ "$headless" = "n" ]; then
	  echo "Output bytes allows you to gauge forward progress."
  fi

  echo "Monitor output with 'tail -f $tmpDir/ant-compile-output.txt'"

  (ant resolve compile-test > $tmpDir/ant-compile-output.txt 2>&1) &

  pid=$!

  if [ "$headless" = "n" ]; then
    progress $pid $tmpDir/ant-compile-output.txt &
  fi

  wait $pid
  if [ $? -ne 0 ]; then
    cat $tmpDir/ant-compile-output.txt
    echo "Compile failed"
    exit 1
  fi
else
  echo "Skipping compile..."
fi

if [ -z "${testClass}" ]; then
  echo "No test name to run was supplied."
  if [ "$cleanAndCompile" = "y" ]; then
    echo "-c was supplied so exit with success"
    exit 0
  fi
  exit 1
fi


echo "Changing to directory: ${baseDir}"
cd "${baseDir}"

echo "Changing to module directory: ${module}"
cd "${module}"

echo -e "\n\nBeasting started..."
echo "Sanity check test launch by looking at 'tail -f $resultsDir/1/1/stdout' and 'tail -f $resultsDir/1/1/stderr'"
echo "Monitor partial results with 'tail -f $resultsDir/results.log' "

if [ "$headless" = "n" ]; then
  # if guake is available, we open the results automatically
  open_guake_tabs &
fi

# add --verbose for debugging
parallel --no-notice --results "${resultsDir}" --progress --jobs $parr --joblog $resultsDir/results.log --tmpdir $tmpDir/parallel-tmp "ant $BEAST_JAVA_OPTS -Divy.resolution-cache.dir=$tmpDir/{1}/ivy-cache -Divy.sync=false -Dtests.workDir=$tmpDir/{1} -Dtests.cachedir=$tmpDir/{1}/test-caches -Dlocal.caches=$tmpDir/{1}/local-caches -Djava.io.tmpdir=$tmpDir/{1}/tmp -Dsolr.skip.sync-hack=true -Dtestcase=$testClass test-nocompile" > /dev/null ::: $(eval echo {1..$iters})

success=$? 

echo -e "\nResults (ExitVal > 0 indicates test fail):\n"

cut -c1-80 $resultsDir/results.log

if [ $success -ne 0 ]; then
  echo -e "\nBeasting was a failure"
  exit 1
fi

echo -e "\nBeasting was a success"
exit 0