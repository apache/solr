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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.noggit.JSONParser;

/**
 * Extraction backend that delegates parsing to a remote Apache Tika Server.
 *
 * <p>This backend uses Java 11 HttpClient to call Tika Server endpoints. It supports
 * backend-neutral extract() and extractOnly() operations. Legacy SAX-based parsing
 * is not supported and will throw UnsupportedOperationException.
 */
public class TikaServerExtractionBackend implements ExtractionBackend {
  private final HttpClient httpClient;
  private final String baseUrl; // e.g., http://localhost:9998
  private final Duration timeout = Duration.ofSeconds(30);

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

  public static final String ID = "tikaserver";

  @Override
  public String name() {
    return ID;
  }

  @Override
  public ExtractionResult extract(InputStream inputStream, ExtractionRequest request)
      throws Exception {
    // 1) Extract text
    String text = requestText(inputStream, request, false, null);

    // 2) Fetch metadata as JSON and convert to neutral metadata
    ExtractionMetadata md = fetchMetadata(request);

    return new ExtractionResult(text, md);
  }

  @Override
  public ExtractionResult extractOnly(
      InputStream inputStream, ExtractionRequest request, String extractFormat, String xpathExpr)
      throws Exception {
    if (xpathExpr != null) {
      throw new UnsupportedOperationException("XPath filtering is not supported by TikaServer backend");
    }
    boolean wantXml = !ExtractingDocumentLoader.TEXT_FORMAT.equalsIgnoreCase(extractFormat);
    String content = requestText(inputStream, request, wantXml, xpathExpr);
    ExtractionMetadata md = fetchMetadata(request);
    return new ExtractionResult(content, md);
  }

  @Override
  public void parseToSolrContentHandler(
      InputStream inputStream,
      ExtractionRequest request,
      SolrContentHandler handler,
      ExtractionMetadata outMetadata)
      throws Exception {
    throw new UnsupportedOperationException(
        "Legacy SAX-based parsing is not supported by TikaServer backend");
  }

  private String requestText(
      InputStream inputStream, ExtractionRequest request, boolean wantXml, String xpath)
      throws IOException, InterruptedException {
    String url = baseUrl + "/tika";
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).POST(HttpRequest.BodyPublishers.ofInputStream(() -> inputStream));
    // Content-Type
    String contentType = firstNonNull(request.streamType, request.contentType);
    if (contentType != null) {
      b.header("Content-Type", contentType);
    }
    // Filename hint
    if (request.resourceName != null) {
      b.header("Content-Disposition", "attachment; filename=\"" + request.resourceName + "\"");
    }
    // Response type
    b.header("Accept", wantXml ? "application/xml" : "text/plain");

    HttpResponse<byte[]> resp = httpClient.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    int code = resp.statusCode();
    if (code < 200 || code >= 300) {
      throw new IOException("TikaServer /tika returned status " + code);
    }
    return new String(resp.body(), StandardCharsets.UTF_8);
  }

  private ExtractionMetadata fetchMetadata(ExtractionRequest request)
      throws IOException, InterruptedException {
    // Call /meta to get metadata. Ask JSON form; Tika Server returns application/json map.
    String url = baseUrl + "/meta";
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).POST(HttpRequest.BodyPublishers.noBody());
    String contentType = firstNonNull(request.streamType, request.contentType);
    if (contentType != null) {
      b.header("Content-Type", contentType);
    }
    if (request.resourceName != null) {
      b.header("Content-Disposition", "attachment; filename=\"" + request.resourceName + "\"");
    }
    b.header("Accept", "application/json");

    HttpResponse<String> resp = httpClient.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    int code = resp.statusCode();
    if (code < 200 || code >= 300) {
      throw new IOException("TikaServer /meta returned status " + code);
    }
    return parseJsonToMetadata(resp.body());
  }

  private static String firstNonNull(String a, String b) {
    return a != null ? a : b;
  }

  // Parse Tika Server metadata JSON using Noggit JSONParser. Supports values as strings,
  // arrays of strings, and basic scalars (numbers/booleans) which are coerced to String.
  private static ExtractionMetadata parseJsonToMetadata(String json) {
    SimpleExtractionMetadata md = new SimpleExtractionMetadata();
    if (json == null) return md;
    try {
      JSONParser p = new JSONParser(json);
      int ev = p.nextEvent();
      if (ev != JSONParser.OBJECT_START) {
        return md;
      }
      String currentKey = null;
      while (true) {
        ev = p.nextEvent();
        if (ev == JSONParser.OBJECT_END || ev == JSONParser.EOF) {
          break;
        }
        if (ev == JSONParser.STRING && p.wasKey()) {
          currentKey = p.getString();
          // Next event is the value for this key
          ev = p.nextEvent();
          if (ev == JSONParser.STRING) {
            md.add(currentKey, p.getString());
          } else if (ev == JSONParser.ARRAY_START) {
            // Read array elements
            while (true) {
              ev = p.nextEvent();
              if (ev == JSONParser.ARRAY_END) break;
              if (ev == JSONParser.STRING) {
                md.add(currentKey, p.getString());
              } else if (ev == JSONParser.LONG
                  || ev == JSONParser.NUMBER
                  || ev == JSONParser.BIGNUMBER) {
                md.add(currentKey, p.getNumberChars().toString());
              } else if (ev == JSONParser.BOOLEAN) {
                md.add(currentKey, String.valueOf(p.getBoolean()));
              } else if (ev == JSONParser.NULL) {
                // ignore nulls
              } else {
                // skip nested objects or unsupported types within arrays
              }
            }
          } else if (ev == JSONParser.LONG
              || ev == JSONParser.NUMBER
              || ev == JSONParser.BIGNUMBER) {
            md.add(currentKey, p.getNumberChars().toString());
          } else if (ev == JSONParser.BOOLEAN) {
            md.add(currentKey, String.valueOf(p.getBoolean()));
          } else if (ev == JSONParser.NULL) {
            // ignore nulls
          } else {
            // skip nested objects or unsupported value types
          }
        }
      }
    } catch (java.io.IOException ioe) {
      // Fall back to empty metadata on parsing error
      return md;
    }
    return md;
  }
}
