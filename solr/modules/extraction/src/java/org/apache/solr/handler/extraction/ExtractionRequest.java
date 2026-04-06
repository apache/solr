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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

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
  public final LinkedHashMap<Pattern, String> passwordsMap;
  public final String extractFormat;

  // Below variables are only used by TikaServerExtractionBackend
  public final boolean tikaServerRecursive;
  public final Integer tikaServerTimeoutSeconds; // optional per-request override
  public final Map<String, String> tikaServerRequestHeaders = new HashMap<>();

  /**
   * Constructs an ExtractionRequest object containing metadata and configurations for extraction
   * backends. This constructor is private - use {@link #builder()} to create instances.
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
   * @param tikaServerRecursive a flag indicating whether extraction should be recursive. TikaServer
   *     only
   * @param tikaServerTimeoutSeconds optional per-request timeout override in seconds (TikaServer
   *     only). If null or ≤ 0, the default timeout will be used
   * @param tikaServerRequestHeaders optional headers to be included in requests to the extraction
   *     service. TikaServer only
   */
  private ExtractionRequest(
      String streamType,
      String resourceName,
      String contentType,
      String charset,
      String streamName,
      String streamSourceInfo,
      Long streamSize,
      String resourcePassword,
      LinkedHashMap<Pattern, String> passwordsMap,
      String extractFormat,
      boolean tikaServerRecursive,
      Integer tikaServerTimeoutSeconds,
      Map<String, String> tikaServerRequestHeaders) {
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
    this.tikaServerRecursive = tikaServerRecursive;
    this.tikaServerTimeoutSeconds = tikaServerTimeoutSeconds;
    if (tikaServerRequestHeaders != null) {
      this.tikaServerRequestHeaders.putAll(tikaServerRequestHeaders);
    }
  }

  /** Creates a new Builder for constructing ExtractionRequest instances. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating ExtractionRequest instances with improved readability and safety. */
  public static class Builder {
    private String streamType;
    private String resourceName;
    private String contentType;
    private String charset;
    private String streamName;
    private String streamSourceInfo;
    private Long streamSize;
    private String resourcePassword;
    private LinkedHashMap<Pattern, String> passwordsMap;
    private String extractFormat;
    private boolean tikaServerRecursive = false;
    private Integer tikaServerTimeoutSeconds;
    private Map<String, String> tikaServerRequestHeaders;

    private Builder() {}

    public Builder streamType(String streamType) {
      this.streamType = streamType;
      return this;
    }

    public Builder resourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public Builder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public Builder charset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder streamName(String streamName) {
      this.streamName = streamName;
      return this;
    }

    public Builder streamSourceInfo(String streamSourceInfo) {
      this.streamSourceInfo = streamSourceInfo;
      return this;
    }

    public Builder streamSize(Long streamSize) {
      this.streamSize = streamSize;
      return this;
    }

    public Builder resourcePassword(String resourcePassword) {
      this.resourcePassword = resourcePassword;
      return this;
    }

    public Builder passwordsMap(LinkedHashMap<Pattern, String> passwordsMap) {
      this.passwordsMap = passwordsMap;
      return this;
    }

    public Builder extractFormat(String extractFormat) {
      this.extractFormat = extractFormat;
      return this;
    }

    public Builder tikaServerRecursive(boolean tikaServerRecursive) {
      this.tikaServerRecursive = tikaServerRecursive;
      return this;
    }

    public Builder tikaServerTimeoutSeconds(Integer tikaServerTimeoutSeconds) {
      this.tikaServerTimeoutSeconds = tikaServerTimeoutSeconds;
      return this;
    }

    public Builder tikaServerRequestHeaders(Map<String, String> tikaServerRequestHeaders) {
      this.tikaServerRequestHeaders = tikaServerRequestHeaders;
      return this;
    }

    public ExtractionRequest build() {
      return new ExtractionRequest(
          streamType,
          resourceName,
          contentType,
          charset,
          streamName,
          streamSourceInfo,
          streamSize,
          resourcePassword,
          passwordsMap,
          extractFormat,
          tikaServerRecursive,
          tikaServerTimeoutSeconds,
          tikaServerRequestHeaders);
    }
  }
}
