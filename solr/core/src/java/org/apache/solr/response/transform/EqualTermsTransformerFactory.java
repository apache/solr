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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;

/**
 * Factory for {@link EqualTermsDocTransformer} instances.
 *
 * <p>Syntax: {fieldName}:equalterms({sourceField},{compareValue})
 *
 * <p>Params:
 *
 * <ul>
 *   <li>sourceField: The field from the document to compare
 *   <li>compareValue: The literal string value to compare with
 * </ul>
 */
public class EqualTermsTransformerFactory extends TransformerFactory {

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
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
    
    IndexSchema schema = req.getSchema();
    
    try {
      return new EqualTermsDocTransformer(field, sourceField, compareValue, schema);
    } catch (IOException e) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Error creating EqualTermsDocTransformer: " + e.getMessage(), e);
    }
  }
}
