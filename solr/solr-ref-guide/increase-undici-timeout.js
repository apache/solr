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
 * Preload script to increase undici HTTP timeout for Antora UI bundle downloads.
 *
 * This script sets the global HTTP agent with longer timeout values to prevent
 * timeout errors when downloading the UI bundle from slow or high-latency networks,
 * particularly in CI environments.
 *
 * Issue: Node.js 20+ includes undici with default timeouts that may be too short
 * for downloading the Antora UI bundle (925KB) from Apache's nightlies server.
 *
 * Usage: This script is preloaded via NODE_OPTIONS in the buildLocalAntoraSite task.
 */

// Configuration via environment variable
const timeoutMs = parseInt(process.env.UNDICI_TIMEOUT_MS || '60000', 10);

// Configure HTTP agent timeouts
try {
  const http = require('node:http');
  const https = require('node:https');

  http.globalAgent.timeout = timeoutMs;
  https.globalAgent.timeout = timeoutMs;

  console.log(`[undici-timeout] Configured HTTP agent timeouts: ${timeoutMs}ms`);
} catch (error) {
  console.warn('[undici-timeout] Could not configure HTTP agent timeouts:', error.message);
}
