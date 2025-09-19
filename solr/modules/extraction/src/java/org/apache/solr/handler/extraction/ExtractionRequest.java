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

/** Immutable request info needed by extraction backends. */
public class ExtractionRequest {
  public final String streamType; // explicit MIME type (optional)
  public final String resourceName; // filename hint
  public final String contentType; // HTTP content-type header
  public final String charset; // derived charset if available
  public final String streamName;
  public final String streamSourceInfo;
  public final Long streamSize;
  public final String resourcePassword; // optional password for encrypted docs
  public final java.util.LinkedHashMap<java.util.regex.Pattern, String>
      passwordsMap; // optional passwords map

  public ExtractionRequest(
      String streamType,
      String resourceName,
      String contentType,
      String charset,
      String streamName,
      String streamSourceInfo,
      Long streamSize,
      String resourcePassword,
      java.util.LinkedHashMap<java.util.regex.Pattern, String> passwordsMap) {
    this.streamType = streamType;
    this.resourceName = resourceName;
    this.contentType = contentType;
    this.charset = charset;
    this.streamName = streamName;
    this.streamSourceInfo = streamSourceInfo;
    this.streamSize = streamSize;
    this.resourcePassword = resourcePassword;
    this.passwordsMap = passwordsMap;
  }
}
