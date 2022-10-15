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

package org.apache.solr.security.jwt.api;

import java.util.List;
import java.util.Map;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class JWTConfigurationPayload implements ReflectMapWriter {

  @JsonProperty public Boolean blockUnknown;

  @JsonProperty public String principalClaim;

  @JsonProperty public Boolean requireExp;

  @JsonProperty public List<String> algAllowlist;

  @JsonProperty public Long jwkCacheDur;

  @JsonProperty public Map<String, Object> claimsMatch;

  // TODO Should this be a List<String> instead of a whitespace delimited string?
  @JsonProperty public String scope;

  @JsonProperty public String realm;

  // TODO Should this be a List<String> instead of a whitespace delimited string?
  @JsonProperty public String rolesClaim;

  @JsonProperty public String adminUiScope;

  @JsonProperty public List<String> redirectUris;

  @JsonProperty public Boolean requireIss;

  @JsonProperty public List<Issuer> issuers;

  @JsonProperty public String trustedCertsFile;

  @JsonProperty public List<String> trustedCerts;

  // Prior to supporting an array of issuers, legacy syntax supported properties for a single issuer
  // at the top-level.
  @JsonProperty public String name;
  @JsonProperty public List<String> jwksUrl;
  @JsonProperty public Map<String, Object> jwk;
  @JsonProperty public String iss;
  @JsonProperty public String aud;
  @JsonProperty public String wellKnownUrl;
  @JsonProperty public String authorizationEndpoint;
  @JsonProperty public String clientId;

  public static class Issuer implements ReflectMapWriter {
    @JsonProperty public String name;

    @JsonProperty public String wellKnownUrl;

    @JsonProperty public String clientId;

    @JsonProperty public List<String> jwksUrl;

    @JsonProperty public Map<String, Object> jwk;

    @JsonProperty public String iss;

    @JsonProperty public String aud;

    @JsonProperty public String authorizationEndpoint;
  }
}
