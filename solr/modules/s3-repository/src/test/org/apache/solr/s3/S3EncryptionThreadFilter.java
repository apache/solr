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

package org.apache.solr.s3;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Ignores the AWS SDK's JVM-wide response stream timeout scheduler, which the S3 encryption client
 * starts on every read. It is a daemon thread that exits on its own 60s after its last task.
 */
public class S3EncryptionThreadFilter implements ThreadFilter {
  @Override
  public boolean reject(Thread t) {
    return "response-input-stream-timeout-scheduler".equals(t.getName());
  }
}
