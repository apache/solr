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

package org.apache.solr.client.solrj.impl;

import static org.apache.solr.common.util.Utils.getObjectByPath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;

public abstract class HttpSolrClientBase extends SolrClient {

  protected static final String DEFAULT_PATH = ClientUtils.DEFAULT_PATH;
  protected static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
  private static final List<String> errPath = Arrays.asList("metadata", "error-class");

  /** The URL of the Solr server. */
  protected final String serverBaseUrl;

  protected final long idleTimeoutMillis;

  protected final long requestTimeoutMillis;

  protected final Set<String> urlParamNames;

  protected RequestWriter requestWriter = new BinaryRequestWriter();

  // updating parser instance needs to go via the setter to ensure update of defaultParserMimeTypes
  protected ResponseParser parser = new BinaryResponseParser();

  protected Set<String> defaultParserMimeTypes;

  protected final String basicAuthAuthorizationStr;

  protected HttpSolrClientBase(String serverBaseUrl, HttpSolrClientBuilderBase<?, ?> builder) {
    if (serverBaseUrl != null) {
      if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
      }
      this.serverBaseUrl = serverBaseUrl;
    } else {
      this.serverBaseUrl = null;
    }
    if (builder.idleTimeoutMillis != null) {
      this.idleTimeoutMillis = builder.idleTimeoutMillis;
    } else {
      this.idleTimeoutMillis = -1;
    }
    this.basicAuthAuthorizationStr = builder.basicAuthAuthorizationStr;
    if (builder.requestWriter != null) {
      this.requestWriter = builder.requestWriter;
    }
    if (builder.responseParser != null) {
      this.parser = builder.responseParser;
    }
    this.defaultCollection = builder.defaultCollection;
    if (builder.requestTimeoutMillis != null) {
      this.requestTimeoutMillis = builder.requestTimeoutMillis;
    } else {
      this.requestTimeoutMillis = -1;
    }
    if (builder.urlParamNames != null) {
      this.urlParamNames = builder.urlParamNames;
    } else {
      this.urlParamNames = Set.of();
    }
  }

  protected String getRequestUrl(SolrRequest<?> solrRequest, String collection)
      throws MalformedURLException {
    return ClientUtils.buildRequestUrl(solrRequest, requestWriter, serverBaseUrl, collection);
  }

  protected ResponseParser responseParser(SolrRequest<?> solrRequest) {
    // TODO add invariantParams support
    return solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();
  }

  // TODO: Remove this for 10.0, there is a typo in the method name
  @Deprecated(since = "9.8", forRemoval = true)
  protected ModifiableSolrParams initalizeSolrParams(
      SolrRequest<?> solrRequest, ResponseParser parserToUse) {
    return initializeSolrParams(solrRequest, parserToUse);
  }

  protected ModifiableSolrParams initializeSolrParams(
      SolrRequest<?> solrRequest, ResponseParser parserToUse) {
    // The parser 'wt=' and 'version=' params are used instead of the original
    // params
    ModifiableSolrParams wparams = new ModifiableSolrParams(solrRequest.getParams());
    wparams.set(CommonParams.WT, parserToUse.getWriterType());
    wparams.set(CommonParams.VERSION, parserToUse.getVersion());
    return wparams;
  }

  protected boolean isMultipart(Collection<ContentStream> streams) {
    boolean isMultipart = false;
    if (streams != null) {
      boolean hasNullStreamName = false;
      hasNullStreamName = streams.stream().anyMatch(cs -> cs.getName() == null);
      isMultipart = !hasNullStreamName && streams.size() > 1;
    }
    return isMultipart;
  }

  protected ModifiableSolrParams calculateQueryParams(
      Set<String> queryParamNames, ModifiableSolrParams wparams) {
    ModifiableSolrParams queryModParams = new ModifiableSolrParams();
    if (queryParamNames != null) {
      for (String param : queryParamNames) {
        String[] value = wparams.getParams(param);
        if (value != null) {
          for (String v : value) {
            queryModParams.add(param, v);
          }
          wparams.remove(param);
        }
      }
    }
    return queryModParams;
  }

  protected void validateGetRequest(SolrRequest<?> solrRequest) throws IOException {
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
    Collection<ContentStream> streams =
        contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;
    if (contentWriter != null || streams != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
    }
  }

  protected abstract boolean isFollowRedirects();

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected NamedList<Object> processErrorsAndResponse(
      int httpStatus,
      String responseReason,
      String responseMethod,
      final ResponseParser processor,
      InputStream is,
      String mimeType,
      String encoding,
      final boolean isV2Api,
      String urlExceptionMessage)
      throws SolrServerException {
    boolean shouldClose = true;
    try {
      // handle some http level checks before trying to parse the response
      switch (httpStatus) {
        case 200: // OK
        case 400: // Bad Request
        case 409: // Conflict
          break;
        case 301: // Moved Permanently
        case 302: // Moved Temporarily
          if (!isFollowRedirects()) {
            throw new SolrServerException(
                "Server at " + urlExceptionMessage + " sent back a redirect (" + httpStatus + ").");
          }
          break;
        default:
          if (processor == null || mimeType == null) {
            throw new BaseHttpSolrClient.RemoteSolrException(
                urlExceptionMessage,
                httpStatus,
                "non ok status: " + httpStatus + ", message:" + responseReason,
                null);
          }
      }

      if (wantStream(processor)) {
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>();
        rsp.add("stream", is);
        rsp.add("responseStatus", httpStatus);
        // Only case where stream should not be closed
        shouldClose = false;
        return rsp;
      }

      checkContentType(processor, is, mimeType, encoding, httpStatus, urlExceptionMessage);

      NamedList<Object> rsp;
      try {
        rsp = processor.processResponse(is, encoding);
      } catch (Exception e) {
        throw new BaseHttpSolrClient.RemoteSolrException(
            urlExceptionMessage, httpStatus, e.getMessage(), e);
      }

      Object error = rsp == null ? null : rsp.get("error");
      if (rsp != null && error == null && processor instanceof NoOpResponseParser) {
        error = rsp.get("response");
      }
      if (error != null
          && (String.valueOf(getObjectByPath(error, true, errPath))
              .endsWith("ExceptionWithErrObject"))) {
        throw BaseHttpSolrClient.RemoteExecutionException.create(urlExceptionMessage, rsp);
      }
      if (httpStatus != 200 && !isV2Api) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          if (error != null) {
            reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("msg"));
            if (reason == null) {
              reason =
                  (String) Utils.getObjectByPath(error, false, Collections.singletonList("trace"));
            }
            Object metadataObj =
                Utils.getObjectByPath(error, false, Collections.singletonList("metadata"));
            if (metadataObj instanceof NamedList) {
              metadata = (NamedList<String>) metadataObj;
            } else if (metadataObj instanceof List) {
              // NamedList parsed as List convert to NamedList again
              List<Object> list = (List<Object>) metadataObj;
              metadata = new NamedList<>(list.size() / 2);
              for (int i = 0; i < list.size(); i += 2) {
                metadata.add((String) list.get(i), (String) list.get(i + 1));
              }
            } else if (metadataObj instanceof Map) {
              metadata = new NamedList((Map) metadataObj);
            }
          }
        } catch (Exception ex) {
          /* Ignored */
        }
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(responseReason).append("\n").append("request: ").append(responseMethod);
          if (error != null) {
            msg.append("\n\nError returned:\n").append(error);
          }
          reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET);
        }
        BaseHttpSolrClient.RemoteSolrException rss =
            new BaseHttpSolrClient.RemoteSolrException(
                urlExceptionMessage, httpStatus, reason, null);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }
      return rsp;
    } finally {
      if (shouldClose) {
        try {
          is.close();
        } catch (IOException e) {
          // quietly
        }
      }
    }
  }

  protected boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  protected abstract boolean processorAcceptsMimeType(
      Collection<String> processorSupportedContentTypes, String mimeType);

  protected abstract String allProcessorSupportedContentTypesCommaDelimited(
      Collection<String> processorSupportedContentTypes);

  /**
   * Validates that the content type in the response can be processed by the Response Parser. Throws
   * a {@code RemoteSolrException} if not.
   */
  private void checkContentType(
      ResponseParser processor,
      InputStream is,
      String mimeType,
      String encoding,
      int httpStatus,
      String urlExceptionMessage) {
    if (mimeType == null
        || (processor == this.parser && defaultParserMimeTypes.contains(mimeType))) {
      // Shortcut the default scenario
      return;
    }
    final Collection<String> processorSupportedContentTypes = processor.getContentTypes();
    if (processorSupportedContentTypes != null && !processorSupportedContentTypes.isEmpty()) {
      boolean processorAcceptsMimeType =
          processorAcceptsMimeType(processorSupportedContentTypes, mimeType);
      if (!processorAcceptsMimeType) {
        // unexpected mime type
        final String allSupportedTypes =
            allProcessorSupportedContentTypesCommaDelimited(processorSupportedContentTypes);
        String prefix =
            "Expected mime type in [" + allSupportedTypes + "] but got " + mimeType + ". ";
        String exceptionEncoding = encoding != null ? encoding : FALLBACK_CHARSET.name();
        try {
          ByteArrayOutputStream body = new ByteArrayOutputStream();
          is.transferTo(body);
          throw new BaseHttpSolrClient.RemoteSolrException(
              urlExceptionMessage, httpStatus, prefix + body.toString(exceptionEncoding), null);
        } catch (IOException e) {
          throw new BaseHttpSolrClient.RemoteSolrException(
              urlExceptionMessage,
              httpStatus,
              "Could not parse response with encoding " + exceptionEncoding,
              e);
        }
      }
    }
  }

  protected static String basicAuthCredentialsToAuthorizationString(String user, String pass) {
    String userPass = user + ":" + pass;
    return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(FALLBACK_CHARSET));
  }

  protected void setParser(ResponseParser parser) {
    this.parser = parser;
    updateDefaultMimeTypeForParser();
  }

  protected abstract void updateDefaultMimeTypeForParser();

  /**
   * @deprecated use {@link #requestAsync(SolrRequest, String)}.
   * @param solrRequest the request to perform
   * @param collection if null the default collection is used
   * @param asyncListener callers should provide an implementation to handle events: start, success,
   *     exception
   * @return Cancellable allowing the caller to attempt cancellation
   */
  @Deprecated
  public abstract Cancellable asyncRequest(
      SolrRequest<?> solrRequest,
      String collection,
      AsyncListener<NamedList<Object>> asyncListener);

  /**
   * Execute an asynchronous request against a Solr server for a given collection.
   *
   * @param request the request to execute
   * @param collection the collection to execute the request against
   * @return a {@link CompletableFuture} that tracks the progress of the async request. Supports
   *     cancelling requests via {@link CompletableFuture#cancel(boolean)}, adding callbacks/error
   *     handling using {@link CompletableFuture#whenComplete(BiConsumer)} and {@link
   *     CompletableFuture#exceptionally(Function)} methods, and other CompletableFuture
   *     functionality. Will complete exceptionally in case of either an {@link IOException} or
   *     {@link SolrServerException} during the request. Once completed, the CompletableFuture will
   *     contain a {@link NamedList} with the response from the server.
   */
  public abstract CompletableFuture<NamedList<Object>> requestAsync(
      final SolrRequest<?> request, String collection);

  /**
   * Execute an asynchronous request against a Solr server using the default collection.
   *
   * @param request the request to execute
   * @return a {@link CompletableFuture} see {@link #requestAsync(SolrRequest, String)}.
   */
  public CompletableFuture<NamedList<Object>> requestAsync(final SolrRequest<?> request) {
    return requestAsync(request, null);
  }

  public boolean isV2ApiRequest(final SolrRequest<?> request) {
    return request.getApiVersion() == SolrRequest.ApiVersion.V2;
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  public ResponseParser getParser() {
    return parser;
  }

  public long getIdleTimeout() {
    return idleTimeoutMillis;
  }

  public Set<String> getUrlParamNames() {
    return urlParamNames;
  }
}
