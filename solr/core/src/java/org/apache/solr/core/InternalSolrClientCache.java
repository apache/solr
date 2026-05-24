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

package org.apache.solr.core;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;

/** A restricted {@link SolrClientCache} that only permits access local solr cluster */
public class InternalSolrClientCache extends SolrClientCache {

  private static final String ALLOW_EXTERNAL_CLUSTERS_PROPERTY = "solr.allow-external-clusters";

  public InternalSolrClientCache(
      HttpSolrClient httpSolrClient, CloudSolrClient.CloudSolrClientConnection solrConnection) {
    super(httpSolrClient);
    cloudSolClients.put(solrConnection, newCloudSolrClient(solrConnection, httpSolrClient, true));
  }

  public InternalSolrClientCache(HttpSolrClient httpSolrClient) {
    super(httpSolrClient);
  }

  @Override
  public synchronized CloudSolrClient getCloudSolrClient(
      CloudSolrClient.CloudSolrClientConnection solrConnection) {
    CloudSolrClient client = cloudSolClients.get(solrConnection);
    if (client != null) {
      return client;
    }
    if (EnvUtils.getPropertyAsBool(ALLOW_EXTERNAL_CLUSTERS_PROPERTY, false)) {
      return super.getCloudSolrClient(solrConnection);
    }
    throw new SolrException(
        SolrException.ErrorCode.FORBIDDEN,
        "External solr cluster is not allowed: "
            + solrConnection
            + ". To allow external clusters set -Dsolr.enable-external-clusters=true "
            + "(WARNING: this may enable SSRF attacks)");
  }
}
