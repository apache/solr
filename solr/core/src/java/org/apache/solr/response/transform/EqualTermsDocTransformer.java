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
import java.util.List;
import java.util.Objects;
import org.apache.lucene.document.StoredField;
import org.apache.solr.common.SolrDocument;

/**
 * Compares a field from a document with a literal string value and adds a boolean field to the
 * document indicating if they are equal.
 *
 * <p>This transformer takes a source field from the index and compares it with a literal string
 * value. The output is a boolean value added to the document that indicates whether the strings are
 * equal.
 *
 * <p>Example usage in a request: fl=id,name,equal:equalterms(name,Smith)
 */
public class EqualTermsDocTransformer extends DocTransformer {
  private final String name;
  private final String sourceField;
  private final String compareValue;

  public EqualTermsDocTransformer(String name, String sourceField, String compareValue) {
    this.name = name;
    this.sourceField = sourceField;
    this.compareValue = compareValue;
  }

  @Override
  public String getName() {
    return name;
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

    boolean isEqual = Objects.equals(fieldValue, compareValue);
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
}
