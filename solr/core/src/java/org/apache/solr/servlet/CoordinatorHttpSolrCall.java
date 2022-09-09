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
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.DelegatedSolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    String sytheticCoreName = factory.collectionVsCoreNameMapping.get(collectionName);
    if (sytheticCoreName != null) {
      return solrCall.cores.getCore(sytheticCoreName);
    } else {
      ZkStateReader zkStateReader = solrCall.cores.getZkController().getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollectionOrNull(collectionName, true);
      if (coll != null) {
        String confName = coll.getConfigName();
        String syntheticCollectionName = SYNTHETIC_COLL_PREFIX + confName;

        DocCollection syntheticColl = clusterState.getCollectionOrNull(syntheticCollectionName);
        if (syntheticColl == null) {
          // no such collection. let's create one
          if (log.isInfoEnabled()) {
            log.info(
                "synthetic collection: {} does not exist, creating.. ", syntheticCollectionName);
          }
          createColl(syntheticCollectionName, solrCall.cores, confName);
        }
        SolrCore core = solrCall.getCoreByCollection(syntheticCollectionName, isPreferLeader);
        if (core != null) {
          factory.collectionVsCoreNameMapping.put(collectionName, core.getName());
          solrCall.cores.getZkController().getZkStateReader().registerCore(collectionName);
          if (log.isDebugEnabled()) {
            log.debug("coordinator node, returns synthetic core: {}", core.getName());
          }
        } else {
          // this node does not have a replica. add one
          if (log.isInfoEnabled()) {
            log.info(
                "this node does not have a replica of the synthetic collection: {} , adding replica ",
                syntheticCollectionName);
          }

          addReplica(syntheticCollectionName, solrCall.cores);
          core = solrCall.getCoreByCollection(syntheticCollectionName, isPreferLeader);
        }
        return core;
      }
      return null;
    }
  }

  private static void addReplica(String syntheticCollectionName, CoreContainer cores) {
    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      cores
          .getCollectionsHandler()
          .handleRequestBody(
              new LocalSolrQueryRequest(
                  null,
                  CollectionAdminRequest.addReplicaToShard(syntheticCollectionName, "shard1")
                      .setCreateNodeSet(cores.getZkController().getNodeName())
                      .getParams()),
              rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not auto-create collection: " + Utils.toJSONString(rsp.getValues()));
      }
    } catch (SolrException e) {
      throw e;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private static void createColl(
      String syntheticCollectionName, CoreContainer cores, String confName) {
    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      SolrParams params =
          CollectionAdminRequest.createCollection(syntheticCollectionName, confName, 1, 1)
              .setCreateNodeSet(cores.getZkController().getNodeName())
              .getParams();
      if (log.isInfoEnabled()) {
        log.info("sending collection admin command : {}", Utils.toJSONString(params));
      }
      cores.getCollectionsHandler().handleRequestBody(new LocalSolrQueryRequest(null, params), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not create :"
                + syntheticCollectionName
                + " collection: "
                + Utils.toJSONString(rsp.getValues()));
      }
    } catch (SolrException e) {
      throw e;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  protected void init() throws Exception {
    super.init();
    if (action == SolrDispatchFilter.Action.PROCESS && core != null) {
      solrReq = wrappedReq(solrReq, collectionName, this);
    }
  }

  public static SolrQueryRequest wrappedReq(
      SolrQueryRequest delegate, String collectionName, HttpSolrCall httpSolrCall) {
    Properties p = new Properties();
    p.put(CoreDescriptor.CORE_COLLECTION, collectionName);
    p.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.PULL.toString());
    p.put(CoreDescriptor.CORE_SHARD, "_");

    CloudDescriptor cloudDescriptor =
        new CloudDescriptor(
            delegate.getCore().getCoreDescriptor(), delegate.getCore().getName(), p);
    return new DelegatedSolrQueryRequest(delegate) {
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
      } else {
        return new CoordinatorHttpSolrCall(this, filter, cores, request, response, retry);
      }
    }
  }
}
