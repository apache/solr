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
 * Preload script to increase HTTP socket timeout for Antora UI bundle downloads.
 *
 * This script intercepts Socket.prototype.setTimeout to enforce a minimum timeout value,
 * preventing timeout errors when downloading the UI bundle from slow or high-latency networks,
 * particularly in CI environments.
 *
 * Usage: This script is preloaded via NODE_OPTIONS in the buildLocalAntoraSite task.
 */

const timeoutMs = parseInt(process.env.HTTP_REQUEST_TIMEOUT_MS || '60000', 10);

try {
  const net = require('node:net');

  // Intercept Socket.prototype.setTimeout to ensure minimum timeout
  const originalSetTimeout = net.Socket.prototype.setTimeout;
  net.Socket.prototype.setTimeout = function(msecs, callback) {
    // If the requested timeout is less than our minimum, use our minimum
    const actualTimeout = Math.max(msecs || 0, timeoutMs);
    if (msecs > 0 && msecs < timeoutMs) {
      console.log(`[http-timeout] Increased socket timeout from ${msecs}ms to ${actualTimeout}ms`);
    }
    return originalSetTimeout.call(this, actualTimeout, callback);
  };

  console.log(`[http-timeout] Socket timeout interceptor initialized with minimum timeout ${timeoutMs}ms`);
} catch (error) {
  console.warn('[http-timeout] Could not configure socket timeouts:', error.message);
}
