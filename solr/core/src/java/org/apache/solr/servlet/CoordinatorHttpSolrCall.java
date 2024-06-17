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

package org.apache.solr.servlet;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.api.CoordinatorV2HttpSolrCall;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SyntheticSolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.DelegatingSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A coordinator node can serve requests as if it hosts all collections in the cluster. it does so
 * by hosting a synthetic replica for each configset used in the cluster.
 *
 * <p>This class is responsible for forwarding the requests to the right core when the node is
 * acting as a Coordinator The responsibilities also involve creating a synthetic collection or
 * replica if they do not exist. It also sets the right threadlocal variables which reflects the
 * current collection being served.
 */
public class CoordinatorHttpSolrCall extends HttpSolrCall {
  public static final String SYNTHETIC_COLL_PREFIX =
      Assign.SYSTEM_COLL_PREFIX + "COORDINATOR-COLL-";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String collectionName;
  private final Factory factory;

  public CoordinatorHttpSolrCall(
      Factory factory,
      SolrDispatchFilter solrDispatchFilter,
      CoreContainer cores,
      HttpServletRequest request,
      HttpServletResponse response,
      boolean retry) {
    super(solrDispatchFilter, cores, request, response, retry);
    this.factory = factory;
  }

  @Override
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    this.collectionName = collectionName;
    SolrCore core = super.getCoreByCollection(collectionName, isPreferLeader);
    if (core != null) return core;
    if (!path.endsWith("/select")) return null;
    return getCore(factory, this, collectionName, isPreferLeader);
  }

  public static SolrCore getCore(
      Factory factory, HttpSolrCall solrCall, String collectionName, boolean isPreferLeader) {
    String syntheticCoreName = factory.collectionVsCoreNameMapping.get(collectionName);
    if (syntheticCoreName != null) {
      SolrCore syntheticCore = solrCall.cores.getCore(syntheticCoreName);
      setMdcLoggingContext(collectionName);
      return syntheticCore;
    } else {
      // first time loading this collection
      ZkStateReader zkStateReader = solrCall.cores.getZkController().getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollectionOrNull(collectionName, true);

      if (coll == null) { // querying on a non-existent collection, it could have been removed
        log.info(
            "Cannot find collection {} to proxy call to, it could have been deleted",
            collectionName);
        return null;
      }

      String confName = coll.getConfigName();
      syntheticCoreName = getSyntheticCoreNameFromConfig(confName);

      SolrCore syntheticCore;
      synchronized (CoordinatorHttpSolrCall.class) {
        CoreContainer coreContainer = solrCall.cores;
        syntheticCore = coreContainer.getCore(syntheticCoreName);
        if (syntheticCore == null) {
          // first time loading this config set
          log.info("Loading synthetic core for config set {}", confName);
          syntheticCore =
              SyntheticSolrCore.createAndRegisterCore(
                  coreContainer, syntheticCoreName, coll.getConfigName());
          // getting the core should open it
          syntheticCore.open();
        }

        factory.collectionVsCoreNameMapping.put(collectionName, syntheticCore.getName());

        // for the watcher, only remove on collection deletion (ie collection == null), since
        // watch from coordinator is collection specific
        coreContainer
            .getZkController()
            .getZkStateReader()
            .registerDocCollectionWatcher(
                collectionName,
                collection -> {
                  if (collection == null) {
                    factory.collectionVsCoreNameMapping.remove(collectionName);
                    return true;
                  } else {
                    return false;
                  }
                });
      }
      setMdcLoggingContext(collectionName);
      if (log.isDebugEnabled()) {
        log.debug("coordinator node, returns synthetic core: {}", syntheticCore.getName());
      }
      return syntheticCore;
    }
  }

  public static String getSyntheticCollectionNameFromConfig(String configName) {
    return SYNTHETIC_COLL_PREFIX + configName;
  }

  public static String getSyntheticCoreNameFromConfig(String configName) {
    return getSyntheticCollectionNameFromConfig(configName) + "_core";
  }

  @Override
  protected void init() throws Exception {
    super.init();
    if (action == SolrDispatchFilter.Action.PROCESS && core != null) {
      solrReq = wrappedReq(solrReq, collectionName, this);
    }
  }

  @Override
  protected String getCoreOrColName() {
    return collectionName;
  }

  public static SolrQueryRequest wrappedReq(
      SolrQueryRequest delegate, String collectionName, HttpSolrCall httpSolrCall) {
    Properties p = new Properties();
    if (collectionName != null) {
      p.put(CoreDescriptor.CORE_COLLECTION, collectionName);
    }
    p.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.PULL.toString());
    p.put(CoreDescriptor.CORE_SHARD, "_");

    CloudDescriptor cloudDescriptor =
        new CloudDescriptor(
            delegate.getCore().getCoreDescriptor(), delegate.getCore().getName(), p);
    return new DelegatingSolrQueryRequest(delegate) {
      @Override
      public HttpSolrCall getHttpSolrCall() {
        return httpSolrCall;
      }

      @Override
      public CloudDescriptor getCloudDescriptor() {
        return cloudDescriptor;
      }
    };
  }

  /**
   * Overrides the MDC context as the core set was synthetic core, which does not reflect the
   * collection being operated on
   */
  private static void setMdcLoggingContext(String collectionName) {
    MDCLoggingContext.setCollection(collectionName);

    // below is irrelevant for call to coordinator
    MDCLoggingContext.setCoreName(null);
    MDCLoggingContext.setShard(null);
    MDCLoggingContext.setCoreName(null);
  }

  // The factory that creates an instance of HttpSolrCall
  public static class Factory implements SolrDispatchFilter.HttpSolrCallFactory {
    private final Map<String, String> collectionVsCoreNameMapping = new ConcurrentHashMap<>();

    @Override
    public HttpSolrCall createInstance(
        SolrDispatchFilter filter,
        String path,
        CoreContainer cores,
        HttpServletRequest request,
        HttpServletResponse response,
        boolean retry) {
      if ((path.startsWith("/____v2/") || path.equals("/____v2"))) {
        return new CoordinatorV2HttpSolrCall(this, filter, cores, request, response, retry);
      } else if (path.startsWith("/" + SYNTHETIC_COLL_PREFIX)) {
        return SolrDispatchFilter.HttpSolrCallFactory.super.createInstance(
            filter, path, cores, request, response, retry);
      } else {
        return new CoordinatorHttpSolrCall(this, filter, cores, request, response, retry);
      }
    }
  }
}
