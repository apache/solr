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

/** Dummy backend that emits predictable test data without actually parsing input content. */
public class DummyExtractionBackend implements ExtractionBackend {
  @Override
  public String name() {
    return "dummy";
  }

  @Override
  public ExtractionResult extract(InputStream inputStream, ExtractionRequest request) {
    ExtractionMetadata metadata = new SimpleExtractionMetadata();
    metadata.add("Dummy-Backend", "true");
    metadata.add(
        "Content-Type",
        request.contentType != null ? request.contentType : "application/octet-stream");
    if (request.resourceName != null) {
      metadata.add("resourcename", request.resourceName);
    }
    String text = "This is dummy extracted content";
    return new ExtractionResult(text, metadata);
  }

  @Override
  public ExtractionResult extractOnly(
      InputStream inputStream, ExtractionRequest request, String extractFormat, String xpathExpr) {
    if (xpathExpr != null) {
      throw new UnsupportedOperationException("XPath not supported by dummy backend");
    }
    return extract(inputStream, request);
  }

  @Override
  public void parseToSolrContentHandler(
      InputStream inputStream,
      ExtractionRequest request,
      SolrContentHandler handler,
      ExtractionMetadata outMetadata) {
    // Fill metadata
    ExtractionResult r = extract(inputStream, request);
    for (String name : r.getMetadata().names()) {
      String[] vals = r.getMetadata().getValues(name);
      if (vals != null) for (String v : vals) outMetadata.add(name, v);
    }
    // Append content
    handler.appendToContent(r.getContent());
  }
}
