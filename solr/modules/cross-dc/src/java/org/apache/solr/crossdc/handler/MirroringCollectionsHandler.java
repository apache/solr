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
package org.apache.solr.crossdc.handler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.crossdc.common.CrossDcConstants;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.crossdc.common.ConfUtil;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MirroringCollectionsHandler extends CollectionsHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Set<String> collections = new HashSet<>();
  private KafkaMirroringSink sink;

  public MirroringCollectionsHandler(CoreContainer coreContainer) {
    this(coreContainer, null);
  }

  public MirroringCollectionsHandler(CoreContainer coreContainer, KafkaMirroringSink sink) {
    super(coreContainer);
    log.info("Using MirroringCollectionsHandler.");
    Map<String, Object> properties = new HashMap<>();
    try {
      SolrZkClient solrClient = coreContainer.getZkController() != null ? coreContainer.getZkController().getZkClient() : null;
      ConfUtil.fillProperties(solrClient, properties);
      ConfUtil.verifyProperties(properties);
      KafkaCrossDcConf conf = new KafkaCrossDcConf(properties);
      String mirrorCollections = conf.get(KafkaCrossDcConf.MIRROR_COLLECTIONS);
      if (mirrorCollections != null && !mirrorCollections.isBlank()) {
        List<String> list = StrUtils.splitSmart(mirrorCollections, ',');
        collections.addAll(list);
      }
      if (sink == null) {
        this.sink = new KafkaMirroringSink(conf);
      } else {
        this.sink = sink;
      }
    } catch (Exception e) {
      log.error("Exception configuring Kafka sink - mirroring disabled!", e);
      this.sink = null;
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    log.debug("-- handler got req params={}", req.getParams());
    DistributedUpdateProcessorFactory.addParamToDistributedRequestWhitelist(req, CrossDcConstants.SHOULD_MIRROR);
    // throw any errors before mirroring
    baseHandleRequestBody(req, rsp);
    if (rsp.getException() != null) {
      return;
    }
    if (sink == null) {
      return;
    }
    boolean doMirroring = req.getParams().getBool(CrossDcConstants.SHOULD_MIRROR, true);
    if (!doMirroring) {
      log.debug(" -- doMirroring=false, skipping...");
      return;
    }
    CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(req.getParams().get(CoreAdminParams.ACTION));
    if (action == null) {
      log.debug("-- unrecognized action, skipping mirroring. Params: {}", req.getParams());
      return;
    }
    // select collection names only when they are mirrored
    if (!collections.isEmpty()) {
      String collection;
      if (action == CollectionParams.CollectionAction.CREATE) {
        collection = req.getParams().get(CommonParams.NAME);
      } else {
        collection = req.getParams().get(CollectionAdminParams.COLLECTION);
      }
      if (!collections.contains(collection)) {
        log.debug("-- collection {} not enabled for mirroring, skipping...", collection);
        return;
      }
    }
    // mirror
    ModifiableSolrParams mirrorParams = ModifiableSolrParams.of(req.getParams());
    // make sure to turn this off to prevent looping
    mirrorParams.set(CrossDcConstants.SHOULD_MIRROR, Boolean.FALSE.toString());
    log.debug("  -- mirroring mirrorParams={}, original responseHeader={}, responseValues={}", mirrorParams, rsp.getResponseHeader(), rsp.getValues());
    SolrRequest solrRequest = new MirroredSolrRequest.MirroredAdminRequest(action, mirrorParams);
    MirroredSolrRequest mirror = new MirroredSolrRequest(MirroredSolrRequest.Type.ADMIN, solrRequest);
    sink.submit(mirror);
  }

  // makes it easier to mock in tests
  @VisibleForTesting
  public void baseHandleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    super.handleRequestBody(req, rsp);
  }
}
