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

# Functionality forked and adapted from https://github.com/apache/spark/blob/v3.2.1/dev/create-release/release-build.sh - Apache license

function LOG() {
  local STATUS=$1
  local MESSAGE=$2
  echo "[$(date)] ${STATUS} ${MESSAGE}"
}

function usage() {
  cat << EOF
Usage: dev-tools/scripts/upload-maven.sh [<options>]
 -v    The version of Solr
 -c    The commit hash for this version of Solr
 -d    Directory where the maven artifacts to upload live.

You must also have the environment variables "ASF_USERNAME" and "ASF_PASSWORD" set.

Example:
  dev-tools/scripts/upload-maven.sh -v 9.0.0 -c asdf23 -d /tmp/release-candidate/maven
EOF
}

while getopts ":hv:c:d:" opt; do
  case ${opt} in
    v)
      SOLR_VERSION="$OPTARG"
      ;;
    c)
      COMMIT_HASH="$OPTARG"
      ;;
    d)
      MAVEN_DIRECTORY="$OPTARG"
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

if [[ -z "$SOLR_VERSION" ]]; then
  LOG INFO "Lacking -v option, must specify Solr version."
  usage
  exit
fi

if [[ -z "$COMMIT_HASH" ]]; then
  LOG INFO "Lacking -c option, must specify Commit Hash."
  usage
  exit
fi

if [[ -z "$MAVEN_DIRECTORY" ]]; then
  LOG INFO "Lacking -d option, must specify directory where maven artifacts live."
  usage
  exit
fi

if [[ -z "$ASF_USERNAME" ]]; then
  LOG ERROR 'The environment variable ASF_USERNAME is not set.'
  usage
  exit
fi

if [[ -z "$ASF_PASSWORD" ]]; then
  LOG ERROR 'The environment variable ASF_PASSWORD is not set.'
  usage
  exit
fi

NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=4bfe5196a41e63 # Profile for Solr staging uploads

# Using Nexus API documented here:
# https://support.sonatype.com/entries/39720203-Uploading-to-a-Staging-Repository-via-REST-API

function create_staging_repo() {
  repo_request="<promoteRequest><data><description>Apache Solr $SOLR_VERSION (commit $COMMIT_HASH)</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u "$ASF_USERNAME:$ASF_PASSWORD" \
    -H "Content-Type:application/xml" -v \
    "$NEXUS_ROOT/profiles/$NEXUS_PROFILE/start")
  staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachesolr-[0-9]\{4\}\).*/\1/")
  echo "$staged_repo_id"
}

function finalize_staging_repo() {
  STAGING_REPO_ID=$1
  repo_request="<promoteRequest><data><stagedRepositoryId>$STAGING_REPO_ID</stagedRepositoryId><description>Apache Solr $SOLR_VERSION (commit $COMMIT_HASH)</description></data></promoteRequest>"
      out=$(curl -X POST -d "$repo_request" -u "$ASF_USERNAME:$ASF_PASSWORD" \
        -H "Content-Type:application/xml" -v \
        "$NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish")
}

echo "Creating Nexus staging repository"
STAGING_REPO_ID="$(create_staging_repo)"
echo "Staging repo created with id: ${STAGING_REPO_ID}"

printf "\n\n"
echo "Uploading files to Nexus staging repository"
(
  cd "${MAVEN_DIRECTORY}"

  NEXUS_FILE_UPLOAD_URL="$NEXUS_ROOT/deployByRepositoryId/$STAGING_REPO_ID"
  for file in $(find . -type f); do
    if [[ $file == *"maven-metadata"* ]]; then
      continue
    fi
    # strip leading ./
    file_short="$(echo "$file" | sed -e "s/\.\///")"
    echo "  Uploading $file_short"
    curl -u "$ASF_USERNAME:$ASF_PASSWORD" --upload-file "$file_short" "$NEXUS_FILE_UPLOAD_URL/$file_short"
  done
)

printf "\n\n"
echo "Finalizing Nexus staging repository"
finalize_staging_repo "${STAGING_REPO_ID}"

printf "\n\n"
LOG "INFO" "SUCCESS on creating staging repository ${STAGING_REPO_ID} for Solr Release ${SOLR_VERSION}"
