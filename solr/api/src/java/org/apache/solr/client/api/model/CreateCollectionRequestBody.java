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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

import static org.apache.solr.client.api.model.Constants.NUM_SLICES;

/** Request body for v2 "create collection" requests */
public class CreateCollectionRequestBody {
    @JsonProperty
    public String name;

    @JsonProperty
    public Integer replicationFactor;

    @JsonProperty
    public String config;

    @JsonProperty(NUM_SLICES)
    @Schema(name = "numShards")
    public Integer numShards;

    @JsonProperty
    public List<String> shardNames;

    @JsonProperty
    public Integer pullReplicas;

    @JsonProperty
    public Integer tlogReplicas;

    @JsonProperty
    public Integer nrtReplicas;

    @JsonProperty
    public Boolean waitForFinalState;

    @JsonProperty
    public Boolean perReplicaState;

    @JsonProperty
    public String alias;

    @JsonProperty
    public Map<String, String> properties;

    @JsonProperty
    public String async;

    // TODO JEGERLOW NOCOMMIT - This line causees a problem in our generated code.  Apparently, custom POJOs like this nested in a request body aren't picked up in the import-list that our api.mustache template operates from.  This is the first time we've really had to tackle this, surprisingly.  There's a similar case in nested POJOs in response objects, but we don't actually need to import those in the generated SolrRequest, as the response object doesn't have the setters/getters for internal fields the way that the request object does.  Options: (1) Try to get a wildcard import working and give up on having generated code pass 'tidy' (2) See if I can modify the spec in such a way that this RouterProperties class gets added to the import list...I wonder how that list is populated.
    @JsonProperty
    public CreateCollectionRouterProperties router;

    // Parameters below differ from v1 API
    // V1 API uses createNodeSet
    @JsonProperty("nodeSet")
    public List<String> nodeSet;
    // v1 API uses createNodeSet=EMPTY
    @JsonProperty("createReplicas")
    public Boolean createReplicas;
    // V1 API uses 'createNodeSet.shuffle'
    @JsonProperty("shuffleNodes")
    public Boolean shuffleNodes;
}
