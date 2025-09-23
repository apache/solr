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

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.noggit.JSONParser;

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
    String url =
        baseUrl
            + "/tika/"
            + (Set.of("html", "xml").contains(request.extractFormat) ? "html" : "text");
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(timeout)
            .header("Accept", "application/json");
    String contentType = firstNonNull(request.streamType, request.contentType);
    if (contentType != null) {
      b.header("Content-Type", contentType);
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

    // TODO: Consider getting the InputStream instead
    HttpResponse<String> resp =
        httpClient.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    int code = resp.statusCode();
    if (code < 200 || code >= 300) {
      // TODO: Parse error message from response?
      throw new SolrException(
          SolrException.ErrorCode.getErrorCode(code),
          "TikaServer " + url + " returned status " + code);
    }
    String body = resp.body();
    return parseCombinedJson(body, md);
  }

  @Override
  public ExtractionResult extractOnly(
      InputStream inputStream, ExtractionRequest request, String xpathExpr) throws Exception {
    if (xpathExpr != null) {
      throw new UnsupportedOperationException(
          "XPath filtering is not supported by TikaServer backend");
    }
    return extract(inputStream, request);
  }

  @Override
  public void parseToSolrContentHandler(
      InputStream inputStream,
      ExtractionRequest request,
      SolrContentHandler handler,
      ExtractionMetadata outMetadata) {
    throw new UnsupportedOperationException(
        "Legacy SAX-based parsing is not supported by TikaServer backend");
  }

  private static String firstNonNull(String a, String b) {
    return a != null ? a : b;
  }

  // Reads key-values of the current object into md. Assumes the parser is positioned
  // right after OBJECT_START of that object.
  private static ExtractionMetadata parseMetadataObject(JSONParser p) throws java.io.IOException {
    ExtractionMetadata md = new ExtractionMetadata();
    String currentKey;
    while (true) {
      int ev = p.nextEvent();
      if (ev == JSONParser.OBJECT_END || ev == JSONParser.EOF) {
        break;
      }
      if (ev == JSONParser.STRING && p.wasKey()) {
        currentKey = p.getString();
        ev = p.nextEvent();
        if (ev == JSONParser.STRING) {
          md.add(currentKey, p.getString());
        } else if (ev == JSONParser.ARRAY_START) {
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
        } else if (ev == JSONParser.LONG || ev == JSONParser.NUMBER || ev == JSONParser.BIGNUMBER) {
          md.add(currentKey, p.getNumberChars().toString());
        } else if (ev == JSONParser.BOOLEAN) {
          md.add(currentKey, String.valueOf(p.getBoolean()));
        } else if (ev == JSONParser.NULL) {
          // ignore nulls
        } else if (ev == JSONParser.OBJECT_START) {
          // Unexpected nested object; skip it entirely
          skipObject(p);
        } else {
          // skip unsupported value types
        }
      }
    }
    return md;
  }

  private static void skipObject(JSONParser p) throws java.io.IOException {
    int depth = 1;
    while (depth > 0) {
      int ev = p.nextEvent();
      if (ev == JSONParser.OBJECT_START) depth++;
      else if (ev == JSONParser.OBJECT_END) depth--;
      else if (ev == JSONParser.EOF) break;
    }
  }

  // Parses combined JSON from /tika/text with Accept: application/json and returns both content
  // and metadata. Supports two shapes:
  // 1) {"content": "...", "metadata": { ... }}
  // 2) {"content": "...", <flat metadata fields> }
  private ExtractionResult parseCombinedJson(String json, ExtractionMetadata md) {
    String content = "";
    if (json == null) return new ExtractionResult(content, md);
    try {
      JSONParser p = new JSONParser(json);
      int ev = p.nextEvent();
      if (ev != JSONParser.OBJECT_START) {
        return new ExtractionResult(content, md);
      }
      while (true) {
        ev = p.nextEvent();
        if (ev == JSONParser.OBJECT_END || ev == JSONParser.EOF) break;
        if (ev == JSONParser.STRING && p.wasKey()) {
          String key = p.getString();
          ev = p.nextEvent();
          if ("X-TIKA:content".equals(key)) {
            if (ev == JSONParser.STRING) {
              content = p.getString();
            } else {
              // Skip non-string content
              if (ev == JSONParser.OBJECT_START) skipObject(p);
            }
          } else if ("metadata".equals(key)) {
            if (ev == JSONParser.OBJECT_START) {
              md = parseMetadataObject(p);
            } else {
              // unexpected shape; skip
              if (ev == JSONParser.OBJECT_START) skipObject(p);
            }
          } else {
            // Treat as flat metadata field
            if (ev == JSONParser.STRING) {
              md.add(key, p.getString());
            } else if (ev == JSONParser.ARRAY_START) {
              while (true) {
                ev = p.nextEvent();
                if (ev == JSONParser.ARRAY_END) break;
                if (ev == JSONParser.STRING) md.add(key, p.getString());
                else if (ev == JSONParser.LONG
                    || ev == JSONParser.NUMBER
                    || ev == JSONParser.BIGNUMBER) md.add(key, p.getNumberChars().toString());
                else if (ev == JSONParser.BOOLEAN) md.add(key, String.valueOf(p.getBoolean()));
                else if (ev == JSONParser.NULL) {
                  // ignore
                }
              }
            } else if (ev == JSONParser.LONG
                || ev == JSONParser.NUMBER
                || ev == JSONParser.BIGNUMBER) {
              md.add(key, p.getNumberChars().toString());
            } else if (ev == JSONParser.BOOLEAN) {
              md.add(key, String.valueOf(p.getBoolean()));
            } else if (ev == JSONParser.NULL) {
              // ignore
            } else if (ev == JSONParser.OBJECT_START) {
              // skip nested object for unknown key
              skipObject(p);
            }
          }
        }
      }
    } catch (java.io.IOException ioe) {
      // ignore, return what we have
    }
    Arrays.stream(md.names()).filter(k -> k.startsWith("X-TIKA:Parsed-")).forEach(md::remove);
    return new ExtractionResult(content, md);
  }
}
