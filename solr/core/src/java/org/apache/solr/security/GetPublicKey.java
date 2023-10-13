/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.security;

import javax.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.GetPublicKeyApi;
import org.apache.solr.client.api.model.PublicKeyResponse;
import org.apache.solr.jersey.PermissionName;

/**
 * V2 API implementation to fetch the public key of the receiving node.
 *
 * <p>This API is analogous to the v1 /admin/info/key endpoint.
 */
public class GetPublicKey extends JerseyResource implements GetPublicKeyApi {

  private final SolrNodeKeyPair nodeKeyPair;

  @Inject
  public GetPublicKey(SolrNodeKeyPair nodeKeyPair) {
    this.nodeKeyPair = nodeKeyPair;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.ALL)
  public PublicKeyResponse getPublicKey() {
    final PublicKeyResponse response = instantiateJerseyResponse(PublicKeyResponse.class);
    response.key = nodeKeyPair.getKeyPair().getPublicKeyStr();
    return response;
  }
}
