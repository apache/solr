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
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.StoredField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * Compares a field from a document with a literal string value using Lucene's TokenStream and adds a boolean field 
 * to the document indicating if they are equal after analysis.
 *
 * <p>This transformer takes a source field from the index and compares it with a literal string value,
 * applying the field's analyzer to both. The comparison is done at the term level using TokenStream.
 * The output is a boolean value added to the document that indicates whether the analyzed terms are equal.
 *
 * <p>Example usage in a request: fl=id,subject,isEqual:[equalterms field=subject value='John Smith']
 */
public class EqualTermsDocTransformer extends DocTransformer {
  private final String name;
  private final String sourceField;
  private final String compareValue;
  private final IndexSchema schema;
  private final List<String> compareValueTerms;

  public EqualTermsDocTransformer(String name, String sourceField, String compareValue, IndexSchema schema) 
      throws IOException {
    this.name = name;
    this.sourceField = sourceField;
    this.compareValue = compareValue;
    this.schema = schema;
    
    // Analyze the comparison value up front
    SchemaField field = schema.getField(sourceField);
    FieldType fieldType = field.getType();
    Analyzer analyzer = fieldType.getIndexAnalyzer();
    
    this.compareValueTerms = analyzeToTerms(analyzer, compareValue);
  }

  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public boolean needsSolrIndexSearcher() {
    return true;
  }

  @Override
  public String[] getExtraRequestFields() {
    return new String[] {sourceField};
  }

  @Override
  public void transform(SolrDocument doc, int docid) throws IOException {
    String fieldValue = getFieldValue(doc);
    if (fieldValue == null) {
      doc.setField(name, false);
      return;
    }
    
    SchemaField field = schema.getField(sourceField);
    FieldType fieldType = field.getType();
    Analyzer analyzer = fieldType.getIndexAnalyzer();
    
    List<String> sourceTerms = analyzeToTerms(analyzer, fieldValue);
    
    boolean isEqual = compareTerms(sourceTerms, compareValueTerms);
    doc.setField(name, isEqual);
  }
  
  private String getFieldValue(SolrDocument doc) {
    Object fieldValue = doc.getFieldValue(sourceField);
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
   * Analyzes text using the provided analyzer and returns a list of String terms.
   */
  private List<String> analyzeToTerms(Analyzer analyzer, String text) throws IOException {
    List<String> terms = new ArrayList<>();
    
    try (TokenStream tokenStream = analyzer.tokenStream("", text)) {
      CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();
      
      while (tokenStream.incrementToken()) {
        // Copy the term text since CharTermAttribute will be reused
        terms.add(termAttr.toString());
      }
      
      tokenStream.end();
    }
    
    return terms;
  }
  
  /**
   * Compares two lists of String terms to see if they contain the same terms in the same order.
   */
  private boolean compareTerms(List<String> sourceTerms, List<String> compareTerms) {
    if (sourceTerms.size() != compareTerms.size()) {
      return false;
    }
    
    for (int i = 0; i < sourceTerms.size(); i++) {
      if (!sourceTerms.get(i).equals(compareTerms.get(i))) {
        return false;
      }
    }
    
    return true;
  }
}