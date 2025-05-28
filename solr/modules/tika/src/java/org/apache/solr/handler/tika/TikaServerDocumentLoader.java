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
package org.apache.solr.handler.tika;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.InputStreamRequestContent;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.MimeTypes;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

public class TikaServerDocumentLoader extends ContentStreamLoader {

  private final String tikaServerUrl;
  private final int connectionTimeout;
  private final int socketTimeout;
  private final String idField;
  private final boolean returnMetadata;
  private final String metadataPrefix;
  private final String contentField;
  private final HttpClient httpClient; // Jetty's HttpClient
  private final SolrQueryRequest req;
  private final UpdateRequestProcessor processor;

  public TikaServerDocumentLoader(
      SolrQueryRequest req,
      UpdateRequestProcessor processor,
      String tikaServerUrl,
      int connectionTimeout,
      int socketTimeout,
      String idField,
      boolean returnMetadata,
      String metadataPrefix,
      String contentField,
      HttpClient jettyClient) {
    this.req = req;
    this.processor = processor;
    this.tikaServerUrl = tikaServerUrl;
    this.connectionTimeout = connectionTimeout;
    this.socketTimeout =
        socketTimeout; // Store it, though specific usage might vary with Jetty client
    this.idField = idField;
    this.returnMetadata = returnMetadata;
    this.metadataPrefix = metadataPrefix;
    this.contentField = contentField;
    this.httpClient = jettyClient;
  }

  @Override
  public void load(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      ContentStream stream,
      UpdateRequestProcessor processor)
      throws Exception {
    String fullTikaServerUrl = this.tikaServerUrl;
    if (!fullTikaServerUrl.endsWith("/")) {
      fullTikaServerUrl += "/";
    }
    // Using /rmeta/text to ensure X-TIKA:content has the main textual content.
    fullTikaServerUrl += "rmeta/text";

    Request jettyRequest = this.httpClient.POST(fullTikaServerUrl);
    jettyRequest.accept(MimeTypes.Type.APPLICATION_JSON.asString());

    String reqContentType = stream.getContentType();
    if (reqContentType != null) {
      jettyRequest.headers(headers -> headers.add(HttpHeader.CONTENT_TYPE, reqContentType));
    } else {
      jettyRequest.headers(
          headers -> headers.add(HttpHeader.CONTENT_TYPE, "application/octet-stream"));
    }

    // Apply request-specific timeout using connectionTimeout.
    // The HttpClient instance itself is configured with an idleTimeout (derived from
    // socketTimeout).
    jettyRequest.timeout(this.connectionTimeout, TimeUnit.MILLISECONDS);

    try (InputStream requestBodyStream = stream.getStream()) { // Ensures stream is closed
      jettyRequest.body(new InputStreamRequestContent(requestBodyStream));

      ContentResponse response = jettyRequest.send(); // Blocking send

      int statusCode = response.getStatus();
      if (statusCode < 200 || statusCode >= 300) {
        String responseContent = response.getContentAsString();
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Tika Server returned HTTP error "
                + statusCode
                + " for "
                + fullTikaServerUrl
                + ". Response: "
                + responseContent);
      }

      String jsonString = response.getContentAsString();
      Object parsedJson = ObjectBuilder.getVal(new JSONParser(jsonString));

      if (!(parsedJson instanceof List)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected JSON response format from Tika Server (expected an array). Got: "
                + jsonString);
      }

      if (!(parsedJson instanceof List<?>)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected JSON response format from Tika Server (expected an array). Got: "
                + parsedJson);
      }

      List<?> tikaOutput = (List<?>) parsedJson;
      if (tikaOutput.isEmpty()) {
        return;
      }

      if (!(tikaOutput.getFirst() instanceof Map)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Expected first element of Tika JSON array to be a Map, got: " + tikaOutput.getFirst());
      }

      if (!(tikaOutput.getFirst() instanceof Map)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Expected first element of Tika JSON array to be a Map, got: " + tikaOutput.getFirst());
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> firstPart = (Map<String, Object>) tikaOutput.getFirst();

      SolrInputDocument sdoc = new SolrInputDocument();

      Object contentValue = firstPart.get("X-TIKA:content");
      if (contentValue != null) {
        sdoc.addField(this.contentField, contentValue.toString());
      }

      if (this.idField != null && !this.idField.trim().isEmpty()) {
        Object idValue = firstPart.get(this.idField);
        if (idValue != null) {
          sdoc.setField(CommonParams.ID, idValue.toString());
        }
      }

      // Restore metadata processing logic based on previous successful tests
      if (this.returnMetadata) {
        for (Map.Entry<String, Object> entry : firstPart.entrySet()) {
          String key = entry.getKey();
          Object value = entry.getValue();

          if (key.equals("X-TIKA:content")) {
            continue; // Content is handled above
          }
          // Add all other fields with prefix. If a field was used as the ID,
          // it will also be added here with a prefix, matching previous behavior.
          String solrFieldName = this.metadataPrefix + key;
          if (value instanceof List) {
            for (Object v : (List<?>) value) {
              sdoc.addField(solrFieldName, v);
            }
          } else {
            sdoc.addField(solrFieldName, value);
          }
        }
      }

      AddUpdateCommand cmd = new AddUpdateCommand(this.req); // Use this.req from constructor
      cmd.solrDoc = sdoc;
      this.processor.processAdd(cmd); // Use this.processor from constructor

    } catch (SolrException se) {
      throw se;
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error processing document with Tika Server: " + e.getMessage(),
          e);
    }
    // requestBodyStream is closed by try-with-resources
  }
}
