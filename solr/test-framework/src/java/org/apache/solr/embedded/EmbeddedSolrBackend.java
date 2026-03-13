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
package org.apache.solr.embedded;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.SolrBackend;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;

/**
 * {@link SolrBackend} backed by an in-process {@link CoreContainer}/{@link EmbeddedSolrServer}. No
 * network or ZooKeeper overhead, although there's some request/response serialization.
 *
 * <p>Data is persisted in the given {@code solrHome} directory.
 */
public class EmbeddedSolrBackend implements SolrBackend {

  private final CoreContainer coreContainer;
  private final EmbeddedSolrServer adminClient;

  public EmbeddedSolrBackend(Path solrHome) {
    coreContainer = new CoreContainer(solrHome, new Properties());
    coreContainer.load();
    adminClient = new EmbeddedSolrServer(coreContainer, null);
  }

  /**
   * @lucene.internal
   */
  public EmbeddedSolrBackend(EmbeddedSolrServer solrServer) {
    this.coreContainer = solrServer.getCoreContainer();
    this.adminClient = solrServer;
  }

  @Override
  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

  @Override
  public SolrClient newClient(String collection) {
    return new EmbeddedSolrServer(coreContainer, collection);
  }

  @Override
  public EmbeddedSolrServer getAdminClient() {
    return adminClient;
  }

  @Override
  public void createCollection(CollectionAdminRequest.Create create) {
    String coreName = create.getCollectionName();
    Map<String, String> coreParams = new HashMap<>();
    if (create.getConfigName() != null) {
      coreParams.put("configSet", create.getConfigName());
    }
    if (create.getProperties() != null) {
      create.getProperties().forEach((k, v) -> coreParams.put(k.toString(), v.toString()));
    }
    coreContainer.create(coreName, coreParams);
  }

  @Override
  public boolean hasCollection(String name) {
    return coreContainer.getCoreDescriptor(name) != null;
  }

  @Override
  public void reloadCollection(String name) throws SolrException {
    coreContainer.reload(name);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(adminClient);
    coreContainer.shutdown();
  }
}
