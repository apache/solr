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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.tika.language.detect.LanguageConfidence;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identifies the language of a set of input fields using Tika's LanguageIdentifier. The
 * tika-core-x.y.jar must be on the classpath
 *
 * <p>See <a
 * href="https://solr.apache.org/guide/solr/latest/indexing-guide/language-detection.html#configuring-tika-language-detection">https://solr.apache.org/guide/solr/latest/indexing-guide/language-detection.html#configuring-tika-language-detection</a>
 *
 * @since 3.5
 */
public class TikaLanguageIdentifierUpdateProcessor extends LanguageIdentifierUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TikaLanguageIdentifierUpdateProcessor(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  @Override
  protected List<DetectedLanguage> detectLanguage(Reader solrDocReader) {
    String content = SolrInputDocumentReader.asString(solrDocReader);
    List<DetectedLanguage> languages = new ArrayList<>();
    if (content.length() != 0) {
      try {
        LanguageDetector detector = LanguageDetector.getDefaultLanguageDetector();
        detector.loadModels();
        detector.addText(content);
        LanguageResult result = detector.detect();

        if (result != null) {
          // Convert LanguageConfidence enum to a numeric certainty value
          Double certainty = 0d;
          LanguageConfidence confidence = result.getConfidence();

          if (confidence != null) {
            switch (confidence) {
              case HIGH:
                certainty = 0.9d;
                break;
              case MEDIUM:
                certainty = 0.5d;
                break;
              case LOW:
                certainty = 0.2d;
                break;
              default:
                certainty = 0.0d;
            }
          }

          DetectedLanguage language = new DetectedLanguage(result.getLanguage(), certainty);
          languages.add(language);

          if (log.isDebugEnabled()) {
            log.debug(
                "Language detected as {} with a certainty of {} (Tika language result={})",
                language,
                language.getCertainty(),
                result);
          }
        }
      } catch (IOException e) {
        log.warn("Failed to load language detector models", e);
      }
    } else {
      log.debug("No input text to detect language from, returning empty list");
    }
    return languages;
  }
}
