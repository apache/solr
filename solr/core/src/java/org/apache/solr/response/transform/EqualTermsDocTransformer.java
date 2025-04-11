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
package org.apache.solr.response.transform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.StoredField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

/**
 * @see EqualTermsTransformerFactory
 */
public class EqualTermsDocTransformer extends DocTransformer {
  private final String name;
  private final SchemaField sourceField;
  private final String compareValue;
  private final List<String> compareValueTerms;

  public EqualTermsDocTransformer(String name, SchemaField sourceField, String compareValue) {
    this.name = name;
    this.sourceField = sourceField;
    this.compareValue = compareValue;

    // Analyze the comparison value up front
    FieldType fieldType = sourceField.getType();
    Analyzer analyzer = fieldType.getIndexAnalyzer();

    this.compareValueTerms = analyzeToTerms(analyzer, compareValue);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String[] getExtraRequestFields() {
    return new String[] {sourceField.getName()};
  }

  @Override
  public void transform(SolrDocument doc, int docid) throws IOException {
    String fieldValue = getFieldValue(doc);
    if (fieldValue == null) {
      doc.setField(name, false);
      return;
    }

    FieldType fieldType = sourceField.getType();
    Analyzer analyzer = fieldType.getIndexAnalyzer();

    // Compare terms on-the-fly as we iterate through the token stream
    // This allows early termination as soon as we find a mismatch
    boolean isEqual = compareTokensOnTheFly(analyzer, fieldValue);
    doc.setField(name, isEqual);
  }

  /**
   * Gets the string field value, or null if not present or if found a list of values other than 1.
   */
  private String getFieldValue(SolrDocument doc) {
    Object fieldValue = doc.getFieldValue(sourceField.getName());
    if (fieldValue instanceof List<?> list) {
      if (list.size() != 1) return null;
      fieldValue = list.getFirst();
    }
    if (fieldValue instanceof CharSequence) return fieldValue.toString();
    if (fieldValue instanceof StoredField storedField) {
      return storedField.stringValue();
    }
    return null;
  }

  /**
   * Compares tokens from the analyzed text with the pre-analyzed comparison tokens. Returns false
   * as soon as a mismatch is found.
   */
  private boolean compareTokensOnTheFly(Analyzer analyzer, String text) throws IOException {
    Iterator<String> compareIter = compareValueTerms.iterator();

    try (TokenStream tokenStream = analyzer.tokenStream(sourceField.getName(), text)) {
      CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();

      while (tokenStream.incrementToken()) {
        // Check if we've seen more tokens than in our comparison list
        if (!compareIter.hasNext()) {
          return false; // More tokens in source than in comparison value
        }

        String compareToken = compareIter.next();

        // Compare the current token with the corresponding token in our pre-analyzed list
        if (termAttr.length() != compareToken.length()
            || CharSequence.compare(termAttr, compareToken) != 0) {
          return false; // Token mismatch
        }
      }

      tokenStream.end();
    }

    // Check if we've seen all the tokens in our comparison list
    return !compareIter.hasNext(); // True if no more comparison tokens remain
  }

  /** Analyzes text using the provided analyzer and returns a list of String terms. */
  private List<String> analyzeToTerms(Analyzer analyzer, String text) {
    List<String> terms = new ArrayList<>();

    try (TokenStream tokenStream = analyzer.tokenStream(sourceField.getName(), text)) {
      CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();

      while (tokenStream.incrementToken()) {
        // Copy the term text since CharTermAttribute will be reused
        terms.add(termAttr.toString());
      }

      tokenStream.end();
    } catch (IOException e) {
      throw new RuntimeException(e); // really unexpected for strings (no IO)
    }

    return terms;
  }
}
