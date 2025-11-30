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

package org.apache.solr.client.solrj.jetty;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClientBase;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.OutputStreamRequestContent;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.http.HttpMethod;

/** A ConcurrentUpdate SolrClient using {@link HttpJettySolrClient}. */
public class ConcurrentUpdateJettySolrClient extends ConcurrentUpdateBaseSolrClient {
  protected static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;

  private final HttpJettySolrClient client;

  public static class Builder extends ConcurrentUpdateBaseSolrClient.Builder {
    /**
     * @see org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient.Builder#Builder(String,
     *     HttpSolrClientBase)
     */
    public Builder(String baseUrl, HttpJettySolrClient client) {
      this(baseUrl, client, false);
    }

    /**
     * @see org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient.Builder#Builder(String,
     *     HttpSolrClientBase, boolean)
     */
    public Builder(String baseSolrUrl, HttpJettySolrClient client, boolean closeHttpClient) {
      super(baseSolrUrl, client, closeHttpClient);
      this.idleTimeoutMillis = client.getIdleTimeoutMillis();
    }

    @Override
    public ConcurrentUpdateJettySolrClient build() {
      return new ConcurrentUpdateJettySolrClient(this);
    }
  }

  protected ConcurrentUpdateJettySolrClient(Builder builder) {
    super(builder);
    this.client = (HttpJettySolrClient) builder.getClient();
  }

  @Override
  protected InputStreamResponseListener doSendUpdateStream(
      ConcurrentUpdateBaseSolrClient.Update update) throws IOException, InterruptedException {
    InputStreamResponseListener responseListener;
    try (OutStream out = initOutStream(basePath, update.request(), update.collection())) {
      ConcurrentUpdateBaseSolrClient.Update upd = update;
      while (upd != null) {
        UpdateRequest req = upd.request();
        if (!out.belongToThisStream(req, upd.collection())) {
          // Request has different params or destination core/collection, return to queue
          queue.add(upd);
          break;
        }
        send(out, upd.request(), upd.collection());
        out.flush();

        notifyQueueAndRunnersIfEmptyQueue();
        upd = queue.poll(pollQueueTimeMillis, TimeUnit.MILLISECONDS);
      }
      responseListener = out.getResponseListener();
    }
    return responseListener;
  }

  private static class OutStream implements Closeable {
    private final String origCollection;
    private final SolrParams origParams;
    private final OutputStreamRequestContent content;
    private final InputStreamResponseListener responseListener;
    private final boolean isXml;

    public OutStream(
        String origCollection,
        SolrParams origParams,
        OutputStreamRequestContent content,
        InputStreamResponseListener responseListener,
        boolean isXml) {
      this.origCollection = origCollection;
      this.origParams = origParams;
      this.content = content;
      this.responseListener = responseListener;
      this.isXml = isXml;
    }

    boolean belongToThisStream(SolrRequest<?> solrRequest, String collection) {
      return origParams.equals(solrRequest.getParams())
          && Objects.equals(origCollection, collection);
    }

    public void write(byte[] b) throws IOException {
      this.content.getOutputStream().write(b);
    }

    public void flush() throws IOException {
      this.content.getOutputStream().flush();
    }

    @Override
    public void close() throws IOException {
      if (isXml) {
        write("</stream>".getBytes(FALLBACK_CHARSET));
      }
      this.content.getOutputStream().close();
    }

    // TODO this class should be hidden
    public InputStreamResponseListener getResponseListener() {
      return responseListener;
    }
  }

  private OutStream initOutStream(String baseUrl, UpdateRequest updateRequest, String collection)
      throws IOException {
    String contentType = client.getRequestWriter().getUpdateContentType();
    final SolrParams origParams = updateRequest.getParams();
    ModifiableSolrParams requestParams =
        client.initializeSolrParams(updateRequest, client.responseParser(updateRequest));

    String basePath = baseUrl;
    if (collection != null) basePath += "/" + collection;
    if (!basePath.endsWith("/")) basePath += "/";

    OutputStreamRequestContent content = new OutputStreamRequestContent(contentType);
    Request postRequest =
        client
            .getHttpClient()
            .newRequest(basePath + "update" + requestParams.toQueryString())
            .method(HttpMethod.POST)
            .body(content);
    client.decorateRequest(postRequest, updateRequest, false);
    InputStreamResponseListener responseListener =
        new HttpJettySolrClient.InputStreamReleaseTrackingResponseListener();
    postRequest.send(responseListener);

    boolean isXml = ClientUtils.TEXT_XML.equals(client.getRequestWriter().getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, content, responseListener, isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(FALLBACK_CHARSET));
    }
    return outStream;
  }

  private void send(OutStream outStream, SolrRequest<?> req, String collection) throws IOException {
    assert outStream.belongToThisStream(req, collection);
    client.getRequestWriter().write(req, outStream.content.getOutputStream());
    if (outStream.isXml) {
      // check for commit or optimize
      SolrParams params = req.getParams();
      assert params != null : "params should not be null";
      if (params != null) {
        String fmt = null;
        if (params.getBool(UpdateParams.OPTIMIZE, false)) {
          fmt = "<optimize waitSearcher=\"%s\" />";
        } else if (params.getBool(UpdateParams.COMMIT, false)) {
          fmt = "<commit waitSearcher=\"%s\" />";
        }
        if (fmt != null) {
          byte[] content =
              String.format(
                      Locale.ROOT, fmt, params.getBool(UpdateParams.WAIT_SEARCHER, false) + "")
                  .getBytes(FALLBACK_CHARSET);
          outStream.write(content);
        }
      }
    }
    outStream.flush();
  }
}
