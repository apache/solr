/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Preload script to increase HTTP request timeout for Antora UI bundle downloads.
 *
 * This script intercepts HTTP/HTTPS requests to set longer timeout values, preventing
 * timeout errors when downloading the UI bundle from slow or high-latency networks,
 * particularly in CI environments.
 *
 * Issue: Antora's UI bundle downloads (925KB from Apache's nightlies server) can timeout
 * on slow networks. The simple-get library used by Antora doesn't set request timeouts
 * by default, so we intercept http.request() and https.request() to add them.
 *
 * Usage: This script is preloaded via NODE_OPTIONS in the buildLocalAntoraSite task.
 */

// Configuration via environment variable
const timeoutMs = parseInt(process.env.HTTP_REQUEST_TIMEOUT_MS || '60000', 10);

// Configure HTTP request timeouts by intercepting request methods
try {
  const http = require('node:http');
  const https = require('node:https');

  // Intercept http.request to add timeout to each request
  const originalHttpRequest = http.request;
  http.request = function(...args) {
    const req = originalHttpRequest.apply(this, args);
    req.setTimeout(timeoutMs);
    console.log(`[http-timeout] Modified http request to set timeout ${timeoutMs}ms`);
    return req;
  };

  // Intercept https.request to add timeout to each request
  const originalHttpsRequest = https.request;
  https.request = function(...args) {
    const req = originalHttpsRequest.apply(this, args);
    req.setTimeout(timeoutMs);
    console.log(`[http-timeout] Modified https request to set timeout ${timeoutMs}ms`);
    return req;
  };
} catch (error) {
  console.warn('[http-timeout] Could not configure HTTP request timeouts:', error.message);
}
