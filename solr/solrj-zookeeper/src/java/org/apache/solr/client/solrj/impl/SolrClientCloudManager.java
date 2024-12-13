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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that implements {@link SolrCloudManager} using a SolrClient */
public class SolrClientCloudManager implements SolrCloudManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CloudHttp2SolrClient cloudSolrClient;
  private final ZkDistribStateManager stateManager;
  private final ZkStateReader zkStateReader;
  private final SolrZkClient zkClient;
  private final ObjectCache objectCache;
  private final boolean closeObjectCache;
  private volatile boolean isClosed;

  public SolrClientCloudManager(CloudHttp2SolrClient client, ObjectCache objectCache) {
    this.cloudSolrClient = client;
    this.zkStateReader = ZkStateReader.from(client);
    this.zkClient = zkStateReader.getZkClient();
    this.stateManager = new ZkDistribStateManager(zkClient);
    this.isClosed = false;
    if (objectCache == null) {
      this.objectCache = new ObjectCache();
      closeObjectCache = true;
    } else {
      this.objectCache = objectCache;
      this.closeObjectCache = false;
    }
  }

  @Override
  public void close() {
    isClosed = true;
    if (closeObjectCache) {
      IOUtils.closeQuietly(objectCache);
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return TimeSource.NANO_TIME;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return cloudSolrClient.getClusterStateProvider();
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return new SolrClientNodeStateProvider(cloudSolrClient);
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return stateManager;
  }

  @Override
  public <T extends SolrResponse> T request(SolrRequest<T> req) throws IOException {
    try {
      return req.process(cloudSolrClient);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  private static final byte[] EMPTY = new byte[0];

  public SolrZkClient getZkClient() {
    return zkClient;
  }
}
