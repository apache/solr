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
package org.apache.solr.client.solrj.response;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.solrj.request.json.JacksonContentWriter;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class holds the response from V2 "/node/info/system" */
public class SystemInfoV2Response extends SystemInfoResponse {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long serialVersionUID = 1L;

  public SystemInfoV2Response(NamedList<Object> namedList) {
    if (namedList == null) throw new IllegalArgumentException("Null NamedList is not allowed.");
    setResponse(namedList);
  }

  /** Parse the V2 response, with "nodeInfo" wrapper */
  @SuppressWarnings("unchecked")
  @Override
  protected void parseResponse(NamedList<Object> response) {
    log.info("V2 response: {}", response);
    Map<String, Object> info = (Map<String, Object>) response.get("nodeInfo");
    String nodeName = (String) info.get("node");
    nodesInfo.put(
        nodeName,
        JacksonContentWriter.DEFAULT_MAPPER.convertValue(
            info, NodeSystemResponse.NodeSystemInfo.class));
  }
}
