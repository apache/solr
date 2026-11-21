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
import org.apache.solr.SolrTestCase;
import org.junit.Test;
import org.xml.sax.helpers.DefaultHandler;

/** SOLR-7498: an unknown stream size must not be reported as the text "null". */
public class ExtractionBackendMetadataTest extends SolrTestCase {

  private static final ExtractionBackend BACKEND =
      new ExtractionBackend() {
        @Override
        public ExtractionResult extract(InputStream inputStream, ExtractionRequest request) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void extractWithSaxHandler(
            InputStream inputStream,
            ExtractionRequest request,
            ExtractionMetadata md,
            DefaultHandler saxContentHandler) {
          throw new UnsupportedOperationException();
        }

        @Override
        public String name() {
          return "test";
        }
      };

  @Test
  public void testUnknownStreamSizeIsNotAdded() {
    ExtractionRequest request = ExtractionRequest.builder().streamName("doc.pdf").build();
    ExtractionMetadata md = BACKEND.buildMetadataFromRequest(request);
    assertNull(md.getFirst(ExtractingMetadataConstants.STREAM_SIZE));
    assertEquals("doc.pdf", md.getFirst(ExtractingMetadataConstants.STREAM_NAME));
  }

  @Test
  public void testKnownStreamSizeIsAdded() {
    ExtractionRequest request = ExtractionRequest.builder().streamSize(1234L).build();
    ExtractionMetadata md = BACKEND.buildMetadataFromRequest(request);
    assertEquals("1234", md.getFirst(ExtractingMetadataConstants.STREAM_SIZE));
  }
}
