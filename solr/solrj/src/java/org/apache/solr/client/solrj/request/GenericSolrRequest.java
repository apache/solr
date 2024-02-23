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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter.ContentWriter;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

public class GenericSolrRequest extends SolrRequest<SimpleSolrResponse> {
  public SolrParams params;
  public SimpleSolrResponse response = new SimpleSolrResponse();
  public ContentWriter contentWriter;
  public boolean requiresCollection;

  /**
   * @param m the HTTP method to use for this request
   * @param path the HTTP path to use for this request. If users are making a collection-aware
   *     request (i.e. {@link #setRequiresCollection(boolean)} is called with 'true'), only the
   *     section of the API path following the collection or core should be provided here.
   */
  public GenericSolrRequest(METHOD m, String path) {
    this(m, path, new ModifiableSolrParams());
  }

  /**
   * @param m the HTTP method to use for this request
   * @param path the HTTP path to use for this request. If users are making a collection-aware
   *     request (i.e. {@link #setRequiresCollection(boolean)} is called with 'true'), only the
   *     section of the API path following the collection or core should be provided here.
   * @param params query parameter names and values for making this request.
   */
  public GenericSolrRequest(METHOD m, String path, SolrParams params) {
    super(m, path);
    this.params = params;
  }

  /**
   * Determines whether the SolrRequest should use a default collection or core from the client
   *
   * <p>Should generally be 'true' whenever making a request to a particular collection or core, and
   * 'false' otherwise.
   *
   * @param requiresCollection true if a default collection should be used, false otherwise.
   */
  public GenericSolrRequest setRequiresCollection(boolean requiresCollection) {
    this.requiresCollection = requiresCollection;
    return this;
  }

  @Override
  public boolean requiresCollection() {
    return requiresCollection;
  }

  public GenericSolrRequest setContentWriter(ContentWriter contentWriter) {
    this.contentWriter = contentWriter;
    return this;
  }

  public GenericSolrRequest withContent(byte[] buf, String type) {
    contentWriter =
        new ContentWriter() {
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
  public ContentWriter getContentWriter(String expectedType) {
    return contentWriter;
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  protected SimpleSolrResponse createResponse(SolrClient client) {
    return response;
  }

  @Override
  public String getRequestType() {
    return SolrRequestType.UNSPECIFIED.toString();
  }
}
