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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.security.PermissionNameProvider.Name.UPDATE_PERM;

import java.util.Iterator;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.NDJsonLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * All v2 APIs that share a prefix of /update
 *
 * <p>Most of these v2 APIs are implemented as pure "pass-throughs" to the v1 code paths, but there
 * are a few exceptions: /update and /update/json are both rewritten to /update/json/docs, unless
 * /update is given newline delimited JSON, in which case it is rewritten to /update/ndjson.
 */
public class UpdateAPI {
  private final UpdateRequestHandler updateRequestHandler;

  public UpdateAPI(UpdateRequestHandler updateRequestHandler) {
    this.updateRequestHandler = updateRequestHandler;
  }

  @EndPoint(method = POST, path = "/update", permission = UPDATE_PERM)
  public void update(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req.getContext()
        .put(
            PATH, isNdJson(req) ? UpdateRequestHandler.NDJSON_PATH : UpdateRequestHandler.DOC_PATH);
    updateRequestHandler.handleRequest(req, rsp);
  }

  @EndPoint(method = POST, path = "/update/ndjson", permission = UPDATE_PERM)
  public void updateNdJson(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    updateRequestHandler.handleRequest(req, rsp);
  }

  @EndPoint(method = POST, path = "/update/xml", permission = UPDATE_PERM)
  public void updateXml(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    updateRequestHandler.handleRequest(req, rsp);
  }

  @EndPoint(method = POST, path = "/update/csv", permission = UPDATE_PERM)
  public void updateCsv(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    updateRequestHandler.handleRequest(req, rsp);
  }

  @EndPoint(method = POST, path = "/update/json", permission = UPDATE_PERM)
  public void updateJson(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req.getContext().put(PATH, "/update/json/docs");
    updateRequestHandler.handleRequest(req, rsp);
  }

  @EndPoint(method = POST, path = "/update/bin", permission = UPDATE_PERM)
  public void updateJavabin(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    updateRequestHandler.handleRequest(req, rsp);
  }

  private static boolean isNdJson(SolrQueryRequest req) {
    String contentType = req.getParams().get(UpdateParams.ASSUME_CONTENT_TYPE);
    if (contentType == null) {
      Iterable<ContentStream> streams = req.getContentStreams();
      Iterator<ContentStream> it = streams == null ? null : streams.iterator();
      if (it == null || !it.hasNext()) {
        return false;
      }
      contentType = it.next().getContentType();
    }
    contentType = UpdateRequestHandler.baseContentType(contentType);
    return contentType != null && NDJsonLoader.CONTENT_TYPES.contains(contentType);
  }
}
