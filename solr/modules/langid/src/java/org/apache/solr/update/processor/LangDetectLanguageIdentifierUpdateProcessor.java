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

import io.github.azagniotov.language.Language;
import io.github.azagniotov.language.LanguageDetectionOrchestrator;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identifies the language of a set of input fields using
 * https://github.com/azagniotov/language-detection
 *
 * <p>See <a
 * href="https://solr.apache.org/guide/solr/latest/indexing-guide/language-detection.html">Detecting
 * Languages During Indexing</a> in the Solr Ref Guide
 *
 * @since 3.5
 */
public class LangDetectLanguageIdentifierUpdateProcessor extends LanguageIdentifierUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final LanguageDetectionOrchestrator orchestrator;

  public LangDetectLanguageIdentifierUpdateProcessor(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      UpdateRequestProcessor next,
      LanguageDetectionOrchestrator orchestrator) {
    super(req, rsp, next);
    this.orchestrator = orchestrator;
  }

  /**
   * Detects language(s) from a reader, typically based on some fields in SolrInputDocument Classes
   * wishing to implement their own language detection module should override this method.
   *
   * @param solrDocReader A reader serving the text from the document to detect
   * @return List of detected language(s) according to RFC-3066
   */
  @Override
  protected List<DetectedLanguage> detectLanguage(Reader solrDocReader) {
    String text = SolrInputDocumentReader.asString(solrDocReader);
    List<Language> langlist = orchestrator.detectAll(text);
    ArrayList<DetectedLanguage> solrLangList = new ArrayList<>();
    for (Language l : langlist) {
      solrLangList.add(
          new DetectedLanguage(l.getIsoCode639_1().toString(), (double) l.getProbability()));
    }
    if (solrLangList.isEmpty()) {
      log.debug("Could not determine language, returning empty list");
    }
    return solrLangList;
  }
}
