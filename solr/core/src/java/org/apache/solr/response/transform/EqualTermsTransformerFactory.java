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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Compares two string values using terms from analyzed (tokenized) text, thus a configurable degree
 * of matching based on schema configuration. One string comes from the document, the other is an
 * input. The transformer yields a boolean result as a new field on the document.
 *
 * <p>Example usage in a request: fl=id,isEqual:[equalterms field=subject value='John Smith']
 *
 * <p>Params:
 *
 * <ul>
 *   <li>field: The field from the document with text to compare, and points to the analyzer
 *       configuration in the schema, used for equality comparison.
 *   <li>value: The literal string value to compare with
 * </ul>
 */
public class EqualTermsTransformerFactory extends TransformerFactory {

  // TODO consider an option to choose the analyzer; not necessarily related to the input field's
  //  analysis.

  @Override
  public DocTransformer create(String destField, SolrParams params, SolrQueryRequest req) {
    String sourceField = params.get("field");
    String compareValue = params.get("value");

    if (sourceField == null) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "EqualTermsTransformer requires 'field' parameter");
    }

    if (compareValue == null) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "EqualTermsTransformer requires 'value' parameter");
    }

    return new EqualTermsDocTransformer(
        destField, req.getSchema().getField(sourceField), compareValue);
  }
}
