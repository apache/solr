#!/bin/bash
#
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
#
##################################################################################
#
# Drops a Swagger UI page into a running `./gradlew dev` Solr instance, so you can
# browse and "Try it out" against Solr's v2 (JAX-RS) API in a real browser.
#
# This is for dev/API-exploration use only, NOT for production use. The files it
# writes live under the gradle build directory and are not tracked by git.
#
# Usage:
#    ./dev-tools/scripts/launch-api-explorer.sh [webappDir]
#
# webappDir defaults to solr/packaging/build/dev/server/solr-webapp/webapp, which is
# where `./gradlew dev` puts the admin UI's webapp. Pass a different path if you're
# pointing at some other already-running Solr instance's webapp directory.
#
# Prerequisites:
#  - A Solr instance already running (e.g. via `./gradlew dev` and `bin/solr start`)
#  - npm, to fetch a local copy of swagger-ui-dist (no CDN dependency, so this works
#    offline and avoids cross-origin "Try it out" calls)
#
##################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WEBAPP_DIR="${1:-${REPO_ROOT}/solr/packaging/build/dev/server/solr-webapp/webapp}"
TARGET_DIR="${WEBAPP_DIR}/ui/openapi"

if [[ ! -d "${WEBAPP_DIR}" ]]; then
  echo "error: webapp directory not found: ${WEBAPP_DIR}" >&2
  echo "Build and start a dev Solr first (./gradlew dev && bin/solr start), or pass its webapp dir as an argument." >&2
  exit 1
fi

echo "==> Regenerating the OpenAPI spec (./gradlew :solr:api:resolve)"
"${REPO_ROOT}/gradlew" -p "${REPO_ROOT}/solr/api" resolve -q

SPEC_SRC="$(ls -t "${REPO_ROOT}"/solr/api/build/generated/openapi/solr-openapi-*.json | head -1)"
if [[ -z "${SPEC_SRC}" ]]; then
  echo "error: could not find a generated OpenAPI spec under solr/api/build/generated/openapi/" >&2
  exit 1
fi
echo "    using spec: ${SPEC_SRC}"

echo "==> Fetching a local copy of swagger-ui-dist via npm"
NPM_WORKDIR="$(mktemp -d)"
trap 'rm -rf "${NPM_WORKDIR}"' EXIT
(
  cd "${NPM_WORKDIR}"
  npm init -y -q >/dev/null
  npm install swagger-ui-dist --no-save -q
)
SWAGGER_UI_DIST="${NPM_WORKDIR}/node_modules/swagger-ui-dist"

echo "==> Writing Swagger UI page to ${TARGET_DIR}"
# Static files must live under one of the Admin UI's existing static-resource
# paths (see WEB-INF/web.xml) - anything else falls through to Solr's own
# catch-all servlet and 404s. "/ui/*" is one such path.
mkdir -p "${TARGET_DIR}"
cp "${SWAGGER_UI_DIST}/swagger-ui-bundle.js" \
   "${SWAGGER_UI_DIST}/swagger-ui-standalone-preset.js" \
   "${SWAGGER_UI_DIST}/swagger-ui.css" \
   "${SWAGGER_UI_DIST}/index.css" \
   "${TARGET_DIR}/"

python3 - "${SPEC_SRC}" "${TARGET_DIR}/spec.json" <<'PY'
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
with open(src) as f:
    spec = json.load(f)
# A relative server URL keeps "Try it out" calls same-origin with this page,
# whichever host:port it ends up being served from.
spec["servers"] = [{"url": "/api", "description": "This Solr node"}]
with open(dst, "w") as f:
    json.dump(spec, f, indent=2)
PY

cat > "${TARGET_DIR}/index.html" <<'HTML'
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Solr V2 API Explorer</title>
  <link rel="stylesheet" type="text/css" href="swagger-ui.css">
  <link rel="stylesheet" type="text/css" href="index.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="swagger-ui-bundle.js" charset="UTF-8"></script>
  <script src="swagger-ui-standalone-preset.js" charset="UTF-8"></script>
  <script src="openapi-init.js" charset="UTF-8"></script>
</body>
</html>
HTML

# Solr sends `Content-Security-Policy: script-src 'self' ...` with no
# 'unsafe-inline' on every response, so the init call has to live in its own
# same-origin file - an inline <script> block is silently never executed.
cat > "${TARGET_DIR}/openapi-init.js" <<'JS'
window.ui = SwaggerUIBundle({
  url: "spec.json",
  dom_id: "#swagger-ui",
  deepLinking: true,
  presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
  plugins: [SwaggerUIBundle.plugins.DownloadUrl],
  layout: "StandaloneLayout"
});
JS

echo
echo "Done. With Solr running, open:"
echo "    http://localhost:8983/solr/ui/openapi/index.html"
