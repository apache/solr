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

package org.apache.solr.savedsearch;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

public class SavedSearchSchemaFields {

  private final SchemaField cacheId;
  private final SchemaField queryId;
  private final SchemaField monitorQuery;

  public SavedSearchSchemaFields(IndexSchema indexSchema) {
    this.cacheId = indexSchema.getField(SavedSearchDataValues.CACHE_ID);
    this.queryId = indexSchema.getField(SavedSearchDataValues.QUERY_ID);
    this.monitorQuery = indexSchema.getField(SavedSearchDataValues.MONITOR_QUERY);
  }

  public SchemaField getCacheId() {
    return cacheId;
  }

  public SchemaField getQueryId() {
    return queryId;
  }

  public SchemaField getMonitorQuery() {
    return monitorQuery;
  }
}
