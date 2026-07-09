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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;

/**
 * To be used for field {@link IndexSchema#NEST_PATH_FIELD_NAME} for enhanced nested doc
 * information. By defining a field type, we can encapsulate the configuration here so that the
 * schema is free of it. Alternatively, some notion of "implicit field types" would be cool and a
 * more general way of accomplishing this.
 *
 * @see org.apache.solr.update.processor.NestedUpdateProcessorFactory
 * @since 8.0
 */
public class NestPathField extends StrField {

  private static final Pattern ORDINAL_PATTERN = Pattern.compile("#\\d*");

  @Override
  public void setArgs(IndexSchema schema, Map<String, String> args) {
    args.putIfAbsent("stored", "false"); // flip a default
    args.putIfAbsent("docValues", "true"); // flip a default; necessary for old schemas
    args.putIfAbsent("multiValued", "false"); // flip a default; necessary for old schemas
    args.putIfAbsent("uninvertible", "false"); // flip a default; necessary for old schemas
    super.setArgs(schema, args);
    // Doesn't support these flags; perhaps others too.
    // note: we could support STORED if truly useful but why bother given docValues.
    restrictProps(STORED | MULTIVALUED | UNINVERTIBLE | TOKENIZED);
  }

  /**
   * {@inheritDoc} Overridden to manipulate the indexed form (AKA terms), but not the DV form. The
   * value may look something like {@code /childA#0/childB#23} and we want to strip out the
   * pound-digits aspects.
   *
   * <p>The terms index is used for block-join and docValues is used for returning nested docs in
   * the same structure as given.
   *
   * @see #toInternal(String)
   */
  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    // indexed or docValues or both may be false... albeit we strongly recommend both enabled and
    // the testing of disabling either may be non-existent.
    final IndexableField termField = createField(field, value); // calls toInternal

    if (!field.hasDocValues()) { // note: strongly recommended, however
      return termField == null ? List.of() : List.of(termField);
    }

    final BytesRef bytes = getBytesRef(value); // unmodified, unlike the indexed term
    final IndexableField dvField = new SortedDocValuesField(field.getName(), bytes);

    return termField == null ? List.of(dvField) : List.of(termField, dvField);
  }

  @Override
  public String toInternal(String val) {
    return ORDINAL_PATTERN.matcher(val).replaceAll("");
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    if (externalVal == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Field " + field.getName() + " missing value.  Forgot `v` local-param?");
    }

    if (externalVal.contains("\\")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Field " + field.getName() + " query value contains backslashes ('\\').");
    }

    if (externalVal.isEmpty() || "/".equals(externalVal)) {
      return new BooleanQuery.Builder()
          .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
          .add(field.getType().getExistenceQuery(parser, field), BooleanClause.Occur.MUST_NOT)
          .build();
    }

    if (!externalVal.startsWith("/") || externalVal.endsWith("/")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Field "
              + field.getName()
              + " query value must start with a forward slash ('/') and cannot contain a trailing slash.");
    }

    return super.getFieldQuery(parser, field, externalVal);
  }
}
