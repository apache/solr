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
package org.apache.solr.client.solrj.cloud;

import java.io.IOException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;

/** Base class for overriding some behavior of {@link SolrCloudManager}. */
public class DelegatingCloudManager implements SolrCloudManager {
  protected final SolrCloudManager delegate;

  public DelegatingCloudManager(SolrCloudManager delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("delegate cannot be null");
    }
    this.delegate = delegate;
  }

  @Override
  public CloudSolrClient getSolrClient() {
    return delegate.getSolrClient();
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return delegate.getClusterStateProvider();
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return delegate.getNodeStateProvider();
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return delegate.getDistribStateManager();
  }

  @Override
  public ObjectCache getObjectCache() {
    return delegate.getObjectCache();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public TimeSource getTimeSource() {
    return delegate.getTimeSource();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
