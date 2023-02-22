/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.tracing.TraceUtils;

/** A common parent for "admin" (i.e. container-level) APIs. */
public abstract class AdminAPIBase extends JerseyResource {

  protected final CoreContainer coreContainer;
  protected final SolrQueryRequest solrQueryRequest;
  protected final SolrQueryResponse solrQueryResponse;

  public AdminAPIBase(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    this.coreContainer = coreContainer;
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  protected CoreContainer fetchAndValidateZooKeeperAwareCoreContainer() {
    validateZooKeeperAwareCoreContainer(coreContainer);
    return coreContainer;
  }

  public static void validateZooKeeperAwareCoreContainer(CoreContainer coreContainer) {
    if (coreContainer == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Core container instance missing");
    }

    // Make sure that the core is ZKAware
    if (!coreContainer.isZooKeeperAware()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Solr instance is not running in SolrCloud mode.");
    }
  }

  /**
   * TODO Taken from CollectionsHandler.handleRequestBody, but its unclear where (if ever) this gets
   * cleared.
   */
  public static void recordCollectionForLogAndTracing(
      String collection, SolrQueryRequest solrQueryRequest) {
    MDCLoggingContext.setCollection(collection);
    TraceUtils.setDbInstance(solrQueryRequest, collection);
  }

  public void disableResponseCaching() {
    solrQueryResponse.setHttpCaching(false);
  }
}
