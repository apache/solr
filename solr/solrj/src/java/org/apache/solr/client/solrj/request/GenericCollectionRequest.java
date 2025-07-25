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

package org.apache.solr.client.solrj.request;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * A generic request for sending to a Solr collection or core.
 *
 * @see GenericSolrRequest
 */
public class GenericCollectionRequest extends CollectionRequiringSolrRequest<SimpleSolrResponse> {
  private final SolrParams params; // not null
  private RequestWriter.ContentWriter contentWriter;

  /**
   * @param m the HTTP method
   * @param path the HTTP path following the collection or core
   * @param requestType the type of this request
   * @param params parameter names and values
   */
  public GenericCollectionRequest(
      METHOD m, String path, SolrRequestType requestType, SolrParams params) {
    super(m, path, requestType);
    this.params = Objects.requireNonNull(params);
  }

  public GenericCollectionRequest withContent(byte[] buf, String type) {
    contentWriter =
        new RequestWriter.ContentWriter() {
          @Override
          public void write(OutputStream os) throws IOException {
            os.write(buf);
          }

          @Override
          public String getContentType() {
            return type;
          }
        };
    return this;
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return contentWriter;
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  protected SimpleSolrResponse createResponse(NamedList<Object> namedList) {
    return new SimpleSolrResponse();
  }
}
