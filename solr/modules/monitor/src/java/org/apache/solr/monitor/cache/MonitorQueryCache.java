/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor.cache;

import java.io.IOException;
import org.apache.lucene.monitor.MonitorDataValues;
import org.apache.lucene.monitor.QueryDecomposer;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;

public interface MonitorQueryCache {

  VersionedQueryCacheEntry computeIfStale(
      MonitorDataValues dataValues, SolrMonitorQueryDecoder decoder) throws IOException;

  boolean acceptTerm(String field, BytesRef value);

  QueryDecomposer getDecomposer();
}
