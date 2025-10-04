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
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.xml.sax.helpers.DefaultHandler;

/** Strategy interface for content extraction backends. */
public interface ExtractionBackend {
  /**
   * Extract plain text and metadata from the inputStream. Implementations should not close the
   * inputStream. This API is backend-neutral and does not expose SAX or XML-specific types.
   */
  ExtractionResult extract(InputStream inputStream, ExtractionRequest request) throws Exception;

  /**
   * Perform extraction of text from input stream with SAX handler. Sax handler can be
   * SolrContentHandler, ToTextContentHandler, ToXMLContentHandler, MatchingContentHandler etc
   */
  void extractWithSaxHandler(
      InputStream inputStream,
      ExtractionRequest request,
      ExtractionMetadata md,
      DefaultHandler saxContentHandler)
      throws Exception;

  /** Build ExtractionMetadata from the request context */
  default ExtractionMetadata buildMetadataFromRequest(ExtractionRequest request) {
    ExtractionMetadata md = new ExtractionMetadata();
    md.addIfNotNull(TikaMetadataKeys.RESOURCE_NAME_KEY, request.resourceName);
    md.addIfNotNull(HttpHeaders.CONTENT_TYPE, request.contentType);
    md.addIfNotNull(ExtractingMetadataConstants.STREAM_NAME, request.streamName);
    md.addIfNotNull(ExtractingMetadataConstants.STREAM_SOURCE_INFO, request.streamSourceInfo);
    md.addIfNotNull(ExtractingMetadataConstants.STREAM_SIZE, String.valueOf(request.streamSize));
    md.addIfNotNull(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, request.contentType);
    md.addIfNotNull(HttpHeaders.CONTENT_ENCODING, request.charset);
    return md;
  }

  /** A short name for debugging/config, e.g., "local" or "tikaserver". */
  String name();
}
