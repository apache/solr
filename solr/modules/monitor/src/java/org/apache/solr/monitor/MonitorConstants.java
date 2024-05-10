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

package org.apache.solr.monitor;

import org.apache.lucene.monitor.QueryDecomposer;

public class MonitorConstants {

  private MonitorConstants() {}

  public static final String REVERSE_SEARCH_PARAM_NAME = "reverseSearch";
  public static final String MONITOR_DOCUMENTS_KEY = "monitorDocuments";
  public static final String MONITOR_DOCUMENT_KEY = "monitorDocument";
  public static final String MONITOR_QUERIES_KEY = "queries";
  public static final String MONITOR_QUERIES_RUN = "queriesRun";
  public static final String QUERY_MATCH_TYPE_KEY = "monitorMatchType";
  public static final String MONITOR_OUTPUT_KEY = "monitor";
  public static final String WRITE_TO_DOC_LIST_KEY = "writeToDocList";
  public static final String HITS_KEY = "hits";
  public static final QueryDecomposer QUERY_DECOMPOSER = new QueryDecomposer();
}
