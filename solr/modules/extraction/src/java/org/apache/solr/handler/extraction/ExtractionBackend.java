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

/** Strategy interface for content extraction backends. */
public interface ExtractionBackend {
  /**
   * Extract plain text and metadata from the inputStream. Implementations should not close the
   * inputStream. This API is backend-neutral and does not expose SAX or XML-specific types.
   */
  ExtractionResult extract(InputStream inputStream, ExtractionRequest request) throws Exception;

  /**
   * Perform extractOnly operation. If extractFormat equals ExtractingDocumentLoader.TEXT_FORMAT,
   * return plain text. If XML, return XML body as string. Implementations may support optional
   * xpathExpr; if unsupported and xpathExpr is not null, they should throw
   * UnsupportedOperationException.
   */
  ExtractionResult extractOnly(InputStream inputStream, ExtractionRequest request, String xpathExpr)
      throws Exception;

  /**
   * Parse the content and stream SAX events into the provided SolrContentHandler, while also
   * filling outMetadata with extracted metadata.
   */
  void parseToSolrContentHandler(
      InputStream inputStream,
      ExtractionRequest request,
      SolrContentHandler handler,
      ExtractionMetadata outMetadata)
      throws Exception;

  /** A short name for debugging/config, e.g., "local" or "dummy". */
  String name();
}
