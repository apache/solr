#!/usr/bin/env bash
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

set -e

usage() {
 echo "This script is to support the specific use case of using Hadoop specific libraries for managing Solr snapshot."
 echo "--prepare-snapshot-export <arg>   This command will prepare"
 echo "                                copylistings for the specified"
 echo "                                snapshot. This command should only"
 echo "                                be used only if Solr is deployed"
 echo "                                with Hadoop and collection index"
 echo "                                files are stored on a shared"
 echo "                                file-system e.g. HDFS"
}

distcp_warning() {
  echo "SOLR_USE_DISTCP environment variable is not set. \
        Do you want to use hadoop distcp tool for exporting Solr collection snapshot ?"
}

use_bin_solr_warning() {
  echo "Please use the bin/solr script to execute this command."
}

parse_options() {
  OPTIND=3
  while getopts ":c:d:s:" o ; do
    case "${o}" in
      d)
        destPath=${OPTARG}
        ;;
      s)
        sourcePath=${OPTARG}
        ;;
      c)
        collectionName=${OPTARG}
        ;;
      *)
        echo "Unknown option ${OPTARG}"
        usage 1>&2
        exit 1
        ;;
    esac
  done
}

run_solr_snapshot_tool() {
  JVM="java"
  scriptDir=$(dirname "$0")
  if [ -n "$LOG4J_PROPS" ]; then
    log4j_config="file:${LOG4J_PROPS}"
  else
    log4j_config="file:${scriptDir}/../../../server/resources/log4j2-console.xml"
  fi
  PATH=${JAVA_HOME}/bin:${PATH} ${JVM} ${ZKCLI_JVM_FLAGS} -Dlog4j.configurationFile=${log4j_config} \
  -classpath "${solrLibPath}" org.apache.solr.hdfs.snapshots.SolrOnHdfsSnapshotsTool "$@" 2> /dev/null
}

prepare_snapshot_export() {
  #Make sure to cleanup the temporary files.
  scratch=$(mktemp -d -t solrsnaps.XXXXXXXXXX)
  function finish {
    rm -rf "${scratch}"
  }
  trap finish EXIT

  if hdfs dfs -test -d "${destPath}" ; then
      run_solr_snapshot_tool --prepare-snapshot-export "$@" -t "${scratch}"

      hdfs dfs -mkdir -p "${copyListingDirPath}" > /dev/null
      find "${scratch}" -type f -printf "%f\n" | while read shardId; do
        echo "Copying the copy-listing for $shardId"
        hdfs dfs -copyFromLocal "${scratch}/${shardId}" "${copyListingDirPath}" > /dev/null
      done
  else
    echo "Directory ${destPath} does not exist."
    exit 1
  fi
}

copy_snapshot_files() {
  copylisting_dir_path="$1"

  if hdfs dfs -test -d "${copylisting_dir_path}" ; then
    for shardId in $(hdfs dfs -stat "%n" "${copylisting_dir_path}/*"); do
      oPath="${destPath}/${snapshotName}/snapshot.${shardId}"
      echo "Copying the index files for ${shardId} to ${oPath}"
      ${distCpCmd} -f "${copylisting_dir_path}/${shardId}" "${oPath}" > /dev/null
    done
  else
    echo "Directory ${copylisting_dir_path} does not exist."
    exit 1
  fi
}

collectionName=""
destPath=""
sourcePath=""
cmd="$1"
snapshotName="$2"
copyListingDirPath=""
distCpCmd="${SOLR_DISTCP_CMD:-hadoop distcp}"
scriptDir=$(dirname "$0")
solrLibPath="${SOLR_LIB_PATH:-${scriptDir}/../../../server/solr-webapp/webapp/WEB-INF/lib/*:${scriptDir}/../../../server/lib/ext/*:${scriptDir}/../lib/*}"
case "${cmd}" in
  --create)
    use_bin_solr_warning
    ;;
  --delete)
    use_bin_solr_warning
    ;;
  --list)
    use_bin_solr_warning
    ;;
  --describe)
    use_bin_solr_warning
    ;;
  --prepare-snapshot-export)
    : "${SOLR_USE_DISTCP:? $(distcp_warning)}"

    parse_options "$@"

    : "${destPath:? Please specify destination directory using -d option}"

    copyListingDirPath="${destPath}/copylistings"
    prepare_snapshot_export "${@:2}"
    echo "Done. GoodBye!"
    ;;
  --export)
    if [ -z "${SOLR_USE_DISTCP}" ]; then
      echo "Not using the distcp command"
      use_bin_solr_warning
      echo "Done. GoodBye!"
      exit 0
    fi

    parse_options "$@"

    : "${snapshotName:? Please specify the name of the snapshot}"
    : "${destPath:? Please specify destination directory using -d option}"

    if [ -n "${collectionName}" ] && [ -n "${sourcePath}" ]; then
      echo "The -c and -s options can not be specified together"
      exit 1
    fi

    if [ -z "${collectionName}" ] && [ -z "${sourcePath}" ]; then
      echo "At least one of options (-c or -s) must be specified"
      exit 1
    fi

    if [ -n "${collectionName}" ]; then
      copyListingDirPath="${destPath}/${snapshotName}/copylistings"
      prepare_snapshot_export "${@:2}"
      copy_snapshot_files "${destPath}/${snapshotName}/copylistings"
      hdfs dfs -rm -r -f -skipTrash "${destPath}/${snapshotName}/copylistings" > /dev/null
    else
      copy_snapshot_files "${sourcePath}/copylistings"
      echo "Copying the collection meta-data to ${destPath}/${snapshotName}"
      ${distCpCmd} "${sourcePath}/${snapshotName}/*" "${destPath}/${snapshotName}/" > /dev/null
    fi

    echo "Done. GoodBye!"
    ;;
  --help|-h)
    usage 1>&2
    ;;
  *)
    echo "Unknown command ${cmd}"
    usage 1>&2
    exit 1
esac
