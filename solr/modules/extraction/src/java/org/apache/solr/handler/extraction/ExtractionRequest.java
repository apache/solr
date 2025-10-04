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

import java.util.HashMap;
import java.util.Map;

/** Immutable request info needed by extraction backends. */
public class ExtractionRequest {
  public final String streamType;
  public final String resourceName;
  public final String contentType;
  public final String charset;
  public final String streamName;
  public final String streamSourceInfo;
  public final Long streamSize;
  public final String resourcePassword;
  public final java.util.LinkedHashMap<java.util.regex.Pattern, String> passwordsMap;
  public final String extractFormat;
  // TODO: This is only used by TikaServerExtractionBackend.
  public final boolean recursive;
  // TODO: This is only used by TikaServerExtractionBackend, change to `features` map?
  public final Map<String, String> tikaRequestHeaders = new HashMap<>();

  /**
   * Constructs an ExtractionRequest object containing metadata and configurations for extraction
   * backends.
   *
   * @param streamType the explicit MIME type of the document (optional)
   * @param resourceName the name of the resource, typically a filename hint
   * @param contentType the HTTP content-type header value
   * @param charset the derived character set of the stream if available
   * @param streamName the name of the content stream
   * @param streamSourceInfo additional information about the stream source
   * @param streamSize the size of the stream in bytes
   * @param resourcePassword an optional password used for encrypted documents
   * @param passwordsMap an optional map of regex patterns to passwords for encrypted content
   * @param extractFormat the desired format for extraction output
   * @param recursive a flag indicating whether extraction should be recursive. TikaServer only
   * @param tikaRequestHeaders optional headers to be included in requests to the extraction
   *     service. TikaServer only
   */
  public ExtractionRequest(
      String streamType,
      String resourceName,
      String contentType,
      String charset,
      String streamName,
      String streamSourceInfo,
      Long streamSize,
      String resourcePassword,
      java.util.LinkedHashMap<java.util.regex.Pattern, String> passwordsMap,
      String extractFormat,
      boolean recursive,
      Map<String, String> tikaRequestHeaders) {
    this.streamType = streamType;
    this.resourceName = resourceName;
    this.contentType = contentType;
    this.charset = charset;
    this.streamName = streamName;
    this.streamSourceInfo = streamSourceInfo;
    this.streamSize = streamSize;
    this.resourcePassword = resourcePassword;
    this.passwordsMap = passwordsMap;
    this.extractFormat = extractFormat;
    this.recursive = recursive;
    if (tikaRequestHeaders != null) {
      this.tikaRequestHeaders.putAll(tikaRequestHeaders);
    }
  }
}
