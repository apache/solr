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
import java.io.StringReader;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.internal.csv.CSVParser;
import org.apache.solr.response.JSONWriter;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.response.XMLWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows indexing key-value pairs. Only subclasses of {@link PrimitiveFieldType} are
 * supported for keys and values.
 *
 * <p>Define the mapping value type using {@code subFieldSuffix} or {@code subFieldType}, as for any
 * subclass of {@link AbstractSubTypeFieldType}. The key type is the first found {@link StrField} by
 * default, or it can be defined using {@code keyFieldSuffix} or {@code keyFieldType}.
 *
 * <p>Expected input document would have a CSV field value:<br>
 * {@code <field name="my_mapping"­>"key","value"</field­>}
 *
 * <p>XML output document will show the mapping key as the {@code name} attribute of the type
 * element of the value. A wrapping {@code <mapping>} XML element helps differentiate mappings from
 * regular Solr elements (i.e.: {@code <str>}, {@code <int>}, etc). Refer to unit tests in {@code
 * TestMappingType} for examples.
 *
 * <p>Json outputs only strings, for any type: refer to {@link JSONWriter}, {@link JsonTextWriter}.
 */
public class MappingType extends AbstractSubTypeFieldType {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int KEY = 0;
  protected static final int VALUE = 1;

  private static final int KEY_VALUE_SIZE = 2;

  private static final String KEY_ATTR = "key";
  private static final String VAL_ATTR = "value";

  private static final String KEY_FIELD_SUFFIX = "keyFieldSuffix";
  private static final String KEY_FIELD_TYPE = "keyFieldType";

  private String keyFieldType = null;
  private String keySuffix = null;
  private FieldType keyType = null;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    if (!(subType instanceof PrimitiveFieldType)) {
      throw new UnsupportedOperationException(
          "Unsupported value type in MappingType: " + subType.getClass().getName());
    }

    SolrParams p = new MapSolrParams(args);
    keyFieldType = p.get(KEY_FIELD_TYPE);
    keySuffix = p.get(KEY_FIELD_SUFFIX);
    if (keyFieldType != null) {
      args.remove(KEY_FIELD_TYPE);
      keyType = schema.getFieldTypeByName(keyFieldType.trim());
      keySuffix = POLY_FIELD_SEPARATOR + keyType.typeName;
    } else if (keySuffix != null) {
      args.remove(KEY_FIELD_SUFFIX);
      keyType = schema.getDynamicFieldType("keyField_" + keySuffix);
    } else {
      String strFieldType =
          schema.getFieldTypes().entrySet().stream()
              .filter(e -> (e.getValue() instanceof StrField))
              .findFirst()
              .get()
              .getKey();
      keyType = schema.getFieldTypeByName(strFieldType);
      keySuffix = POLY_FIELD_SEPARATOR + keyType.typeName;
    }

    createSuffixCache(KEY_VALUE_SIZE);
  }

  @Override
  protected void createSuffixCache(int size) {
    suffixes = new String[size];
    suffixes[KEY] = "_key" + keySuffix;
    suffixes[VALUE] = "_value" + suffix;
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    if (keyType != null) {
      SchemaField protoKey = registerDynamicPrototype(schema, keyType, keySuffix, this);
      dynFieldProps += protoKey.getProperties();
    }
    if (subType != null) {
      SchemaField protoVal = registerDynamicPrototype(schema, subType, suffix, this);
      dynFieldProps += protoVal.getProperties();
    }
  }

  private SchemaField registerDynamicPrototype(
      IndexSchema schema, FieldType type, String fieldSuffix, FieldType polyField) {
    String name = "*" + fieldSuffix;
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");
    log.warn(
        "MappingType requires indexed subtypes (key, value).  "
            + "Setting the subtype '{}' to 'indexed=true'",
        type.getTypeName());

    props.put("stored", "false");
    log.warn(
        "MappingType does not support stored on the subtypes (key, value).  "
            + "Setting the subtype '{}' to 'stored=false'",
        type.getTypeName());

    props.put("multiValued", "false");
    log.warn(
        "MappingType does not support multiValued on the subtypes (key, value).  "
            + "Setting the subtype '{}' to 'multiValued=false'",
        type.getTypeName());

    props.put("docValues", "false");
    log.warn(
        "MappingType does not support docValues on the subtypes (key, value).  "
            + "Setting the subtype '{}' to 'docValues=false'",
        type.getTypeName());

    int p = SchemaField.calcProps(name, type, props);
    SchemaField proto = SchemaField.create(name, type, p, null);
    schema.registerDynamicFields(proto);
    return proto;
  }

  @Override
  protected void checkSupportsDocValues() {
    // DocValues supported only when enabled at the fieldType
    if (!hasProperty(DOC_VALUES)) {
      throw new UnsupportedOperationException(
          "MappingType can't have docValues=true in the field definition, use docValues=true in the fieldType definition.");
    }
  }

  @Override
  public boolean isPolyField() {
    return true; // really only true if the field is indexed
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    String externalVal = value.toString();
    String[] csv = parseCommaSeparatedList(externalVal);

    List<IndexableField> fields = new ArrayList<>((suffixes.length * 2) + 1);

    if (field.indexed()) {
      SchemaField keyField = getKeyField(field);
      fields.addAll(keyField.createFields(csv[KEY]));

      SchemaField valField = getValueField(field);
      fields.addAll(valField.createFields(csv[VALUE]));
    }

    if (field.stored()) {
      fields.add(createField(field.getName(), externalVal, StoredField.TYPE));
    }

    if (field.hasDocValues()) {
      fields.add(createDocValuesField(field, value.toString()));
    }

    return fields;
  }

  private IndexableField createDocValuesField(SchemaField field, String value) {
    IndexableField docval;
    final BytesRef bytes = new BytesRef(toInternal(value));
    if (field.multiValued()) {
      docval = new SortedSetDocValuesField(field.getName(), bytes);
    } else {
      docval = new SortedDocValuesField(field.getName(), bytes);
    }
    return docval;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    throw new UnsupportedOperationException(
        "MappingType uses multiple fields.  field=" + field.getName());
  }

  /**
   * Given a string of comma-separated values, return a String array of length 2 containing the
   * values.
   *
   * @param externalVal The value to parse
   * @return An array of the values that make up the mapping
   * @throws SolrException if the input value cannot be parsed
   */
  protected static String[] parseCommaSeparatedList(String externalVal) throws SolrException {
    String[] out = new String[KEY_VALUE_SIZE];
    // input is: "key","value"
    CSVParser parser = new CSVParser(new StringReader(externalVal));
    try {
      String[] tokens = parser.getLine();
      if (tokens != null && tokens.length > 0) {
        out[0] = tokens[0];
        out[1] = tokens.length > 1 ? tokens[1] : "";

      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid input value: " + externalVal);
      }
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to parse as CSV: " + externalVal);
    }
    return out;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    if (log.isTraceEnabled()) {
      log.trace(
          "Write MappingType '{}' as 'mapping' from indexable '{}'",
          name,
          f.getClass().getSimpleName());
    }
    String[] keyVal = parseCommaSeparatedList(f.stringValue());
    if (writer instanceof XMLWriter) {
      doWriteXMl(writer, name, keyVal[KEY], keyVal[VALUE]);
    }
    if (writer instanceof JSONWriter) {
      doWriteJson(writer, name, keyVal[KEY], keyVal[VALUE]);
    }
  }

  private void doWriteJson(TextResponseWriter writer, String name, String key, String val)
      throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put(KEY_ATTR, key);
    map.put(VAL_ATTR, val);
    // JSONWriter, JsonTextWriter : write everything as str.
    writer.writeMap(name, map, false, true);
  }

  private void doWriteXMl(TextResponseWriter writer, String name, String key, String val)
      throws IOException {
    Writer w = writer.getWriter();
    if (writer.doIndent()) {
      writer.indent();
      writer.incLevel();
    }
    w.write("<mapping name=\"");
    w.write(name != null ? name : key);
    w.write("\">");
    writeSubFieldObject(writer, KEY_ATTR, key, keyType.getClass().getSimpleName());
    writeSubFieldObject(writer, VAL_ATTR, val, subType.getClass().getSimpleName());

    if (writer.doIndent()) {
      writer.decLevel();
      writer.indent();
    }
    w.write("</mapping>");
  }

  private void writeSubFieldObject(
      TextResponseWriter writer, String key, String val, String subTypeClsName) throws IOException {
    switch (subTypeClsName) {
      case "StrField":
        writer.writeStr(key, val, true);
        break;
      case "IntPointField":
      case "TrieIntField":
        writer.writeInt(key, val);
        break;
      case "LongPointField":
      case "TrieLongField":
        writer.writeLong(key, val);
        break;
      case "FloatPointField":
      case "TrieFloatField":
        writer.writeFloat(key, val);
        break;
      case "DoublePointField":
      case "TrieDoubleField":
        writer.writeDouble(key, val);
        break;
      case "DatePointField":
      case "TrieDateField":
        writer.writeDate(key, val);
        break;
      case "BoolField":
        writer.writeBool(key, val);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported value type in MappingType: " + subTypeClsName);
    }
  }

  /** Sorting is not supported on {@code MappingType} */
  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Sorting not supported on MappingType " + field.getName());
  }

  /** Uninversion is not supported on {@code MappingType} */
  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  /** Range query is not supported on {@code MappingType} */
  @Override
  protected Query getSpecializedRangeQuery(
      QParser parser,
      SchemaField field,
      String part1,
      String part2,
      boolean minInclusive,
      boolean maxInclusive) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Range query not supported on MappingType " + field.getName());
  }

  public SchemaField getKeyField(SchemaField base) {
    return schema.getField(base.getName() + suffixes[KEY]);
  }

  public SchemaField getValueField(SchemaField base) {
    return schema.getField(base.getName() + suffixes[VALUE]);
  }

  /** Returns a Query to support existence search on the mapping field name */
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    BytesRefBuilder br = new BytesRefBuilder();
    readableToIndexed(externalVal, br);
    return new TermQuery(new Term(field.getName(), br));
  }
}
