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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimerTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorHttpSolrCall extends HttpSolrCall {
  public static final String SYNTHETIC_COLL_PREFIX = "SYNTHETIC-CONF-COLL_";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String collectionName;

  public CoordinatorHttpSolrCall(SolrDispatchFilter solrDispatchFilter, CoreContainer cores, HttpServletRequest request,
                                 HttpServletResponse response, boolean retry) {
    super(solrDispatchFilter, cores, request, response, retry);
  }

  @Override
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    this.collectionName = collectionName;
    SolrCore core = super.getCoreByCollection(collectionName, isPreferLeader);
    if (core != null) return core;
    return getCore(this, collectionName, isPreferLeader);
  }

  @SuppressWarnings("unchecked")
  public static  SolrCore getCore(HttpSolrCall solrCall, String collectionName, boolean isPreferLeader) {
     Map<String, String> coreNameMapping= (Map<String, String>) solrCall.cores.getObjectCache().computeIfAbsent(CoordinatorHttpSolrCall.class.getName(),
             s -> new ConcurrentHashMap<>());
    String sytheticCoreName = coreNameMapping.get(collectionName);
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
          log.info("synthetic collection: {} does not exist, creating.. ", syntheticCollectionName);
          createColl(syntheticCollectionName, solrCall.cores, confName);
        }
        SolrCore core = solrCall.getCoreByCollection(syntheticCollectionName, isPreferLeader);
        if (core != null) {
          coreNameMapping.put(collectionName, core.getName());
          log.info("coordinator NODE , returns synthetic core " + core.getName());
        } else {
          //this node does not have a replica. add one
          log.info("this node does not have a replica of the synthetic collection: {} , adding replica ", syntheticCollectionName);

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
      cores.getCollectionsHandler().handleRequestBody(new LocalSolrQueryRequest(null,
              CollectionAdminRequest.addReplicaToShard(syntheticCollectionName, "shard1")
                      .setCreateNodeSet(cores.getZkController().getNodeName())
                      .getParams()
      ), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not auto-create collection: " + Utils.toJSONString(rsp.getValues()));
      }
    } catch (SolrException e) {
      throw e;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }

  private static void createColl(String syntheticCollectionName, CoreContainer cores, String confName) {
    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      SolrParams params = CollectionAdminRequest.createCollection(syntheticCollectionName, confName, 1, 1)
              .setCreateNodeSet(cores.getZkController().getNodeName()).getParams();
      log.info("sending collection admin command : {}", Utils.toJSONString(params));
      cores.getCollectionsHandler()
              .handleRequestBody(new LocalSolrQueryRequest(null,
                      params), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not create :" + syntheticCollectionName + " collection: " + Utils.toJSONString(rsp.getValues()));
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
    if(action == SolrDispatchFilter.Action.PROCESS && core != null) {
      solrReq = wrappedReq(solrReq, collectionName, this);
    }
  }

  public static SolrQueryRequest wrappedReq(SolrQueryRequest delegate, String collectionName, HttpSolrCall httpSolrCall) {
    Properties p = new Properties();
    p.put(CoreDescriptor.CORE_COLLECTION, collectionName);
    p.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.PULL.toString());
    p.put(CoreDescriptor.CORE_SHARD, "_");

    CloudDescriptor cloudDescriptor =  new CloudDescriptor(delegate.getCore().getCoreDescriptor(),
            delegate.getCore().getName(), p);
     return new SolrQueryRequest() {
      @Override
      public SolrParams getParams() {
        return delegate.getParams();
      }

      @Override
      public void setParams(SolrParams params) {

        delegate.setParams(params);
      }

      @Override
      public Iterable<ContentStream> getContentStreams() {
        return delegate.getContentStreams();
      }

      @Override
      public SolrParams getOriginalParams() {
        return delegate.getOriginalParams();
      }

      @Override
      public Map<Object, Object> getContext() {
        return delegate.getContext();
      }

      @Override
      public void close() {
        delegate.close();
      }

      @Override
      public long getStartTime() {
        return delegate.getStartTime();
      }

      @Override
      public RTimerTree getRequestTimer() {
        return delegate.getRequestTimer();
      }

      @Override
      public SolrIndexSearcher getSearcher() {
        return delegate.getSearcher();
      }

      @Override
      public SolrCore getCore() {
        return delegate.getCore();
      }

      @Override
      public IndexSchema getSchema() {
        return delegate.getSchema();
      }

      @Override
      public void updateSchemaToLatest() {
        delegate.updateSchemaToLatest();

      }

      @Override
      public String getParamString() {
        return delegate.getParamString();
      }

      @Override
      public Map<String, Object> getJSON() {
        return delegate.getJSON();
      }

      @Override
      public void setJSON(Map<String, Object> json) {
        delegate.setJSON(json);
      }

      @Override
      public Principal getUserPrincipal() {
        return delegate.getUserPrincipal();
      }

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
}
