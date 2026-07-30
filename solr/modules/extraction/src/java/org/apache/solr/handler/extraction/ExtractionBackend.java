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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Content extraction backends must implement this interface.
 *
 * <p>Implementations must be thread-safe as a single instance may be shared across multiple
 * concurrent requests.
 */
public interface ExtractionBackend extends Closeable {
  /**
   * Extract plain text and metadata from the inputStream. Implementations should not close the
   * inputStream.
   */
  ExtractionResult extract(InputStream inputStream, ExtractionRequest request) throws Exception;

  /**
   * Perform extraction of text from inputStream with SAX handler. Examples of SAX handlers are
   * SolrContentHandler, ToTextContentHandler, ToXMLContentHandler and MatchingContentHandler.
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
    md.add(ExtractingMetadataConstants.RESOURCE_NAME_KEY, request.resourceName);
    md.add(ExtractingMetadataConstants.HTTP_HEADER_CONTENT_TYPE, request.contentType);
    md.add(ExtractingMetadataConstants.STREAM_NAME, request.streamName);
    md.add(ExtractingMetadataConstants.STREAM_SOURCE_INFO, request.streamSourceInfo);
    md.add(ExtractingMetadataConstants.STREAM_SIZE, String.valueOf(request.streamSize));
    md.add(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, request.contentType);
    md.add(ExtractingMetadataConstants.HTTP_HEADER_CONTENT_ENCODING, request.charset);
    return md;
  }

  /** A short name for debugging/config. */
  String name();

  @Override
  default void close() throws IOException {
    // default no-op; specific backends may override to release shared resources
  }
}
