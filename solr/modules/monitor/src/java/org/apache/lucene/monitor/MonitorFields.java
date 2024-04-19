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

package org.apache.lucene.monitor;

import java.util.Set;

public class MonitorFields {

  public static final String QUERY_ID = QueryIndex.FIELDS.query_id;
  public static final String CACHE_ID = QueryIndex.FIELDS.cache_id;
  public static final String MONITOR_QUERY = QueryIndex.FIELDS.mq;
  public static final String PAYLOAD = QueryIndex.FIELDS.mq + "_payload";
  public static final String VERSION = "_version_";
  public static final String ANYTOKEN_FIELD = TermFilteredPresearcher.ANYTOKEN_FIELD;

  public static final Set<String> RESERVED_MONITOR_FIELDS =
      Set.of(QUERY_ID, CACHE_ID, MONITOR_QUERY, PAYLOAD, VERSION, ANYTOKEN_FIELD);
}
