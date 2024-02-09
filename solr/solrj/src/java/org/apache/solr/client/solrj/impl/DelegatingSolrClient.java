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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.util.NamedList;

/**
 * A {@link SolrClient} implementation that defers to a delegate SolrClient object.
 *
 * <p>Callers are responsible for closing the 'delegate' client themselves. {@link
 * DelegatingSolrClient#close()} is a no-op
 */
public class DelegatingSolrClient extends SolrClient {

  private final SolrClient delegate;

  public DelegatingSolrClient(SolrClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public NamedList<Object> request(SolrRequest<?> request, String collection)
      throws SolrServerException, IOException {
    if (ClientUtils.shouldApplyDefaultCollection(collection, request)) {
      collection = getDefaultCollection();
    }
    return delegate.request(request, collection);
  }

  @Override
  public void close() throws IOException {
    /* No-op */
  }
}
