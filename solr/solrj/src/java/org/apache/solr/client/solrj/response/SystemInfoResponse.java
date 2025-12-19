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
import java.util.Date;
import org.apache.solr.client.solrj.request.json.JacksonContentWriter;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class holds the response from V1 "/admin/info/system" or V2 "/node/system" */
public class SystemInfoResponse extends SolrResponseBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long serialVersionUID = 1L;

  private org.apache.solr.client.api.model.SystemInfoResponse fullResponse;

  public SystemInfoResponse(NamedList<Object> namedList) {
    if (namedList == null) throw new IllegalArgumentException("Null NamedList is not allowed.");
    setResponse(namedList);
  }

  @Override
  public void setResponse(NamedList<Object> response) {
    if (getResponse() == null) super.setResponse(response);
    if (fullResponse == null) {
      try {
        fullResponse =
            JacksonContentWriter.DEFAULT_MAPPER.convertValue(
                getResponse(), org.apache.solr.client.api.model.SystemInfoResponse.class);
      } catch (Throwable t) {
        log.error("Cannot convert NamedList response to API model SystemInfoResponse", t);
      }
    }
  }

  public String getMode() {
    return getFullResponse().mode;
  }

  public String getZkHost() {
    return getFullResponse().zkHost;
  }

  public String getSolrHome() {
    return getFullResponse().solrHome;
  }

  public String getCoreRoot() {
    return getFullResponse().coreRoot;
  }

  public String getNode() {
    return getFullResponse().node;
  }

  public org.apache.solr.client.api.model.SystemInfoResponse getFullResponse() {
    return fullResponse;
  }

  public String getSolrImplVersion() {
    return getFullResponse() != null && getFullResponse().lucene != null
        ? getFullResponse().lucene.solrImplVersion
        : null;
  }

  public String getSolrSpecVersion() {
    return getFullResponse() != null && getFullResponse().lucene != null
        ? getFullResponse().lucene.solrSpecVersion
        : null;
  }

  public Date getJVMStartTime() {
    return getFullResponse() != null
            && getFullResponse().jvm != null
            && getFullResponse().jvm.jmx != null
        ? getFullResponse().jvm.jmx.startTime
        : null;
  }

  public Long getJVMUpTime() {
    return getFullResponse() != null
            && getFullResponse().jvm != null
            && getFullResponse().jvm.jmx != null
        ? getFullResponse().jvm.jmx.upTimeMS
        : null;
  }

  public String getJVMMemoryUsed() {
    return getFullResponse() != null
            && getFullResponse().jvm != null
            && getFullResponse().jvm.memory != null
        ? getFullResponse().jvm.memory.used
        : null;
  }

  public String getJVMMemoryTtl() {
    return getFullResponse() != null
            && getFullResponse().jvm != null
            && getFullResponse().jvm.memory != null
        ? getFullResponse().jvm.memory.total
        : null;
  }
}
