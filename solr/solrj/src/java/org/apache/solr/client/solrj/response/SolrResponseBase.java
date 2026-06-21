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

import java.io.IOException;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.NamedList;

/**
 * 
 *
 * @since solr 1.3
 */
public class SolrResponseBase extends SolrResponse implements MapWriter
{
  private long elapsedTime = -1;
  private final NamedList<Object> response;
  private String requestUrl = null;

//  SolrResponseBase() {
//    this.response = null;
//  }

  public SolrResponseBase(NamedList response) {
    Objects.nonNull(response);
    this.response = response;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (response != null) response.writeMap(ew);
  }

  @Override
  public long getElapsedTime() {
    return elapsedTime;
  }

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  @Override
  public NamedList<Object> getResponse() {
    return response;
  }

  @Override
  public String toString() {
    return response.toString();
  }
  
  public NamedList getResponseHeader() {
    return (NamedList) response.get("responseHeader");
  }
  
  // these two methods are based on the logic in SolrCore.setResponseHeaderValues(...)
  public int getStatus() {
    NamedList header = getResponseHeader();
    if (header != null) {
       Object obj = header.get("status");
       if (obj == null) {
         return 0;
       }
       return (Integer) obj;
    } else {
        return 0;
    }
  }
  
  public int getQTime() {
    NamedList header = getResponseHeader();
    if (header != null) {
        return (Integer) header.get("QTime");
    }
    else {
        return 0;
    }
  }

  public String getRequestUrl() {
    return requestUrl;
  }

  public void setRequestUrl(String requestUrl) {
    this.requestUrl = requestUrl;
  }
  
}
