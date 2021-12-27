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
package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.ArrayUtils;
//import org.apache.lucene.document.VectorField;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.vector.search.SearchVector;
import org.apache.solr.vector.search.VectorQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VectorFieldType extends FieldType {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String vectorDimenssion;
  private static final String PARAM_VECTOR_DIMENSSION = "vectorDimession";

  @Override
  public Type getUninversionType(SchemaField sf) {
    return Type.VECTOR_FIELD;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeArray(name, parse(f.getCharSequenceValue()));
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    //TODO throw exception
    return null;
  }
  
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    this.vectorDimenssion = args.remove(PARAM_VECTOR_DIMENSSION);
  }
  
  private List<Float> parse(CharSequence value)throws IOException {
    if(value.charAt(0) == '[' || value.charAt(0) == '{') {
      ObjectMapper mapper = new ObjectMapper();
      // 1. convert JSON array to Array objects
      List<Float> pp1 = mapper.readValue(value.toString(), new TypeReference<List<Float>>() {});
      return pp1;
    } else {
      String str = value.toString();
      String[] tokens = str.split(" ");
      List<Float> pp1 = new ArrayList<Float>();
      for(int i=0;i<tokens.length;i++) {
        pp1.add(Float.parseFloat(tokens[i]));
      }
      return pp1;
    }
  }
  
  public IndexableField createField(SchemaField field, Object value) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: {}", field);
      return null;
    }
      @SuppressWarnings("unchecked")
      SolrInputField inValue = (SolrInputField) value;
      @SuppressWarnings("unchecked")
      ArrayList<Float> vList = (ArrayList<Float>) inValue.getValue();
      Float[] values = vList.toArray(new Float[vList.size()]);
      return new KnnVectorField(field.getName(),  ArrayUtils.toPrimitive(values), KnnVectorField.createFieldType(values.length, VectorSimilarityFunction.DOT_PRODUCT));
//      return new VectorField(field.getName(),  ArrayUtils.toPrimitive(values), VectorField.createHnswType(values.length, VectorValues.SimilarityFunction.DOT_PRODUCT, 10, 10)); 

  }

  /**
   * Returns a Query instance for doing searches against a field.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @param externalVal The String representation of the value to search
   * @return The {@link org.apache.lucene.search.Query} instance.  This implementation returns a {@link org.apache.solr.vector.search.VectorQuery} but overriding queries may not
   */
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {

    if (field.hasDocValues() && !field.indexed()) {
      // match-only
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      float vector[] = null;
      String arrOfStr[] = externalVal.split(",");
      vector = new float[arrOfStr.length];
      for (int i = 0; i < arrOfStr.length; i++) { 
            vector[i] = Float.parseFloat(arrOfStr[i]);
      }
      return new VectorQuery(new SearchVector(field.getName(), vector));
    }
  }

}
