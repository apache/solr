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
package org.apache.solr.common.params;

/** Size Parameters */
public interface SizeParams {
  public static final String SIZE = "size";
  public static final String AVG_DOC_SIZE = "avgDocSize";
  public static final String NUM_DOCS = "numDocs";
  public static final String DELETED_DOCS = "deletedDocs";
  public static final String FILTER_CACHE_MAX = "filterCacheMax";
  public static final String QUERY_RES_CACHE_MAX = "queryResultCacheMax";
  public static final String DOC_CACHE_MAX = "documentCacheMax";
  public static final String QUERY_RES_MAX_DOCS = "queryResultMaxDocsCached";
  public static final String ESTIMATION_RATIO = "estimationRatio";
  public static final String SIZE_UNIT = "sizeUnit";
}
