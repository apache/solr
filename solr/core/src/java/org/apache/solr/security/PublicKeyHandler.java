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

package org.apache.solr.security;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;

public class PublicKeyHandler extends RequestHandlerBase {
  public static final String PATH = "/admin/info/key";

  private final SolrNodeKeyPair nodeKeyPair;

  @VisibleForTesting
  public PublicKeyHandler() {
    this(new SolrNodeKeyPair(null));
  }

  public PublicKeyHandler(SolrNodeKeyPair nodeKeyPair) {
    this.nodeKeyPair = nodeKeyPair;
  }

  public CryptoKeys.RSAKeyPair getKeyPair() {
    return nodeKeyPair.getKeyPair();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        rsp, new GetPublicKey(nodeKeyPair).getPublicKey());
  }

  @Override
  public String getDescription() {
    return "Return the public key of this server";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.ALL;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Api> getApis() {
    return new ArrayList<>();
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(GetPublicKey.class);
  }
}
