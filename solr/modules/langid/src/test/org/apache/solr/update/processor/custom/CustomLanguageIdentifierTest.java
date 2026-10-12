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
package org.apache.solr.update.processor.custom;

import java.io.Reader;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DetectedLanguage;
import org.apache.solr.update.processor.LanguageIdentifierUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;

/** A language identifier living outside the processor package can build its own results. */
public class CustomLanguageIdentifierTest extends SolrTestCase {

  /** Minimal custom implementation; compiling it is the main check. */
  static class FixedLanguageIdentifier extends LanguageIdentifierUpdateProcessor {
    FixedLanguageIdentifier(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      super(req, rsp, next);
    }

    @Override
    protected List<DetectedLanguage> detectLanguage(Reader solrDocReader) {
      return List.of(new DetectedLanguage("sv", 0.75));
    }
  }

  public void testDetectedLanguageCanBeCreatedOutsidePackage() {
    DetectedLanguage lang = new DetectedLanguage("en", 0.9);
    assertEquals("en", lang.getLangCode());
    assertEquals(0.9, lang.getCertainty(), 0.0);
  }
}
