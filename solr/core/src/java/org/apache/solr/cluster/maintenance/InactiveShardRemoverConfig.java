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

package org.apache.solr.cluster.maintenance;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class InactiveShardRemoverConfig implements ReflectMapWriter {

  public static final long DEFAULT_SCHEDULE_INTERVAL_SECONDS = 900L; // 15 minutes

  public static final long DEFAULT_TTL_SECONDS = 900L; // 15 minutes

  public static final int DEFAULT_MAX_DELETES_PER_CYCLE = 20;

  @JsonProperty public long scheduleIntervalSeconds;

  @JsonProperty public long ttlSeconds;

  @JsonProperty public int maxDeletesPerCycle;

  /** Default constructor required for deserialization */
  public InactiveShardRemoverConfig() {
    this(DEFAULT_SCHEDULE_INTERVAL_SECONDS, DEFAULT_TTL_SECONDS, DEFAULT_MAX_DELETES_PER_CYCLE);
  }

  public InactiveShardRemoverConfig(
      final long scheduleIntervalSeconds, final long ttlSeconds, final int maxDeletesPerCycle) {
    this.scheduleIntervalSeconds = scheduleIntervalSeconds;
    this.ttlSeconds = ttlSeconds;
    this.maxDeletesPerCycle = maxDeletesPerCycle;
  }

  public void validate() {
    if (scheduleIntervalSeconds <= 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "scheduleIntervalSeconds must be greater than 0");
    }
    if (maxDeletesPerCycle <= 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "maxDeletesPerCycle must be greater than 0");
    }
  }
}
