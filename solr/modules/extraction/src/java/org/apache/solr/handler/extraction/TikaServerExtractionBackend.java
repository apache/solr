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
package org.apache.solr.handler.extraction;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.solr.common.SolrException;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Extraction backend that delegates parsing to a remote Apache Tika Server.
 *
 * <p>This backend uses Java 11 HttpClient to call Tika Server endpoints. It supports
 * backend-neutral extract() and extractOnly() operations. Legacy SAX-based parsing is not supported
 * and will throw UnsupportedOperationException.
 */
public class TikaServerExtractionBackend implements ExtractionBackend {
  private final HttpClient httpClient;
  private final String baseUrl; // e.g., http://localhost:9998
  private final Duration timeout = Duration.ofSeconds(30);
  private final TikaServerParser tikaServerResponseParser = new TikaServerParser();

  public TikaServerExtractionBackend(String baseUrl) {
    this(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(), baseUrl);
  }

  // Visible for tests
  TikaServerExtractionBackend(HttpClient httpClient, String baseUrl) {
    if (baseUrl.endsWith("/")) {
      this.baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    } else {
      this.baseUrl = baseUrl;
    }
    this.httpClient = httpClient;
  }

  public static final String NAME = "tikaserver";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public ExtractionResult extract(InputStream inputStream, ExtractionRequest request)
      throws Exception {
    try (InputStream tikaResponse = callTikaServer(inputStream, request)) {
      ExtractionMetadata md = buildMetadataFromRequest(request);
      BodyContentHandler textHandler = new BodyContentHandler(-1);
      if (request.recursive) {
        tikaServerResponseParser.parseRmetaJson(tikaResponse, textHandler, md);
      } else {
        tikaServerResponseParser.parseXml(tikaResponse, textHandler, md);
      }
      return new ExtractionResult(textHandler.toString(), md);
    }
  }

  @Override
  public void extractWithSaxHandler(
      InputStream inputStream,
      ExtractionRequest request,
      ExtractionMetadata md,
      DefaultHandler saxContentHandler)
      throws Exception {
    try (InputStream tikaResponse = callTikaServer(inputStream, request)) {
      if (request.recursive) {
        tikaServerResponseParser.parseRmetaJson(tikaResponse, saxContentHandler, md);
      } else {
        tikaServerResponseParser.parseXml(tikaResponse, saxContentHandler, md);
      }
    }
  }

  private static String firstNonNull(String a, String b) {
    return a != null ? a : b;
  }

  /**
   * Call the Tika Server to extract text and metadata. Depending on request.recursive, will either
   * return XML (false) or JSON array (true)
   *
   * @return InputStream of the response body, either XML or json depending on request.recursive
   */
  private InputStream callTikaServer(InputStream inputStream, ExtractionRequest request)
      throws IOException, InterruptedException {
    String url = baseUrl + (request.recursive ? "/rmeta" : "/tika");
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(timeout)
            .header("Accept", (request.recursive ? "application/json" : "text/xml"));
    String contentType = firstNonNull(request.streamType, request.contentType);
    if (contentType != null) {
      b.header("Content-Type", contentType);
    }
    if (!request.tikaRequestHeaders.isEmpty()) {
      request.tikaRequestHeaders.forEach(b::header);
    }
    ExtractionMetadata md = buildMetadataFromRequest(request);
    if (request.resourcePassword != null || request.passwordsMap != null) {
      RegexRulesPasswordProvider passwordProvider = new RegexRulesPasswordProvider();
      if (request.resourcePassword != null) {
        passwordProvider.setExplicitPassword(request.resourcePassword);
      }
      if (request.passwordsMap != null) {
        passwordProvider.setPasswordMap(request.passwordsMap);
      }

      String pwd = passwordProvider.getPassword(md);
      if (pwd != null) {
        b.header("Password", pwd);
      }
    }
    if (request.resourceName != null) {
      b.header("Content-Disposition", "attachment; filename=\"" + request.resourceName + "\"");
    }
    b.PUT(HttpRequest.BodyPublishers.ofInputStream(() -> inputStream));

    HttpResponse<InputStream> resp =
        httpClient.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
    int code = resp.statusCode();
    if (code < 200 || code >= 300) {
      throw new SolrException(
          SolrException.ErrorCode.getErrorCode(code),
          "TikaServer " + url + " returned status " + code);
    }
    return resp.body();
  }
}
