/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

Scalar.createApiReference("#app", {
  url: "spec.json",
  // Keep fonts local and send API requests directly to Solr.
  withDefaultFonts: false,
  proxyUrl: "",
  agent: { disabled: true },
  hideClientButton: true,
  showDeveloperTools: "never",
  telemetry: false,
  // The Admin UI has no dark mode of its own; keep the explorer visually consistent with it.
  forceDarkModeState: "light",
  hideDarkModeToggle: true,
  hooks: {
    // "Test Request" issues its own fetch() calls, bypassing the Admin UI's Angular/superagent
    // interceptors (see app.js's httpInterceptor and solrApi.ApiClient.instance.plugins), so
    // neither the logged-in credentials nor the popup-suppression header reach Solr here unless
    // we attach them ourselves. Re-read the credentials on every request, exactly like those
    // interceptors do, so login/logout is respected; only ever send them to this origin, never to
    // a different server a user might select or type into the explorer.
    onBeforeRequest({ request }) {
      if (new URL(request.url).origin !== window.location.origin) {
        return;
      }
      var authHeader = sessionStorage.getItem("auth.header");
      if (authHeader) {
        request.headers.set("Authorization", authHeader);
      }
      // Force BasicAuth plugin to serve us a 'Authorization: xBasic xxxx' header so the browser
      // won't pop up its own login dialogue, matching app.js's $httpProvider configuration.
      request.headers.set("X-Requested-With", "XMLHttpRequest");
    }
  }
});
