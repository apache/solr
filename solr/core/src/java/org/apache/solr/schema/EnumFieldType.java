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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.EnumFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedIntFieldSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.util.SafeXMLParsing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/** Field type supporting string values with custom sort order. */
public class EnumFieldType extends PrimitiveFieldType {
  protected EnumMapping enumMapping;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    enumMapping = new EnumMapping(schema, this, args);
  }

  public EnumMapping getEnumMapping() {
    return enumMapping;
  }

  /**
   * Models all the info contained in an enums config XML file
   *
   * @lucene.internal
   */
  public static final class EnumMapping {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String PARAM_ENUMS_CONFIG = "enumsConfig";
    public static final String PARAM_ENUM_NAME = "enumName";
    public static final Integer DEFAULT_VALUE = -1;

    public final Map<String, Integer> enumStringToIntMap;
    public final Map<Integer, String> enumIntToStringMap;

    private final String enumsConfigFile;
    private final String enumName;

    /**
     * Takes in a FieldType and the initArgs Map used for that type, removing the keys that specify
     * the enum.
     *
     * @param schema for opening resources
     * @param fieldType Used for logging or error messages
     * @param args the init args to consume the enum name + config file from
     */
    public EnumMapping(IndexSchema schema, FieldType fieldType, Map<String, String> args) {
      final String ftName = fieldType.getTypeName();

      // NOTE: ghosting member variables for most of constructor
      final Map<String, Integer> enumStringToIntMap = new HashMap<>();
      final Map<Integer, String> enumIntToStringMap = new HashMap<>();

      enumsConfigFile = args.get(PARAM_ENUMS_CONFIG);
      if (enumsConfigFile == null) {
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND, ftName + ": No enums config file was configured.");
      }
      enumName = args.get(PARAM_ENUM_NAME);
      if (enumName == null) {
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND, ftName + ": No enum name was configured.");
      }

      final SolrResourceLoader loader = schema.getResourceLoader();
      try {
        log.debug("Reloading enums config file from {}", enumsConfigFile);
        Document doc = SafeXMLParsing.parseConfigXML(log, loader, enumsConfigFile);
        final XPathFactory xpathFactory = XPathFactory.newInstance();
        final XPath xpath = xpathFactory.newXPath();
        final String xpathStr =
            String.format(Locale.ROOT, "/enumsConfig/enum[@name='%s']", enumName);
        final NodeList nodes = (NodeList) xpath.evaluate(xpathStr, doc, XPathConstants.NODESET);
        final int nodesLength = nodes.getLength();
        if (nodesLength == 0) {
          String exceptionMessage =
              String.format(
                  Locale.ENGLISH,
                  "%s: No enum configuration found for enum '%s' in %s.",
                  ftName,
                  enumName,
                  enumsConfigFile);
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, exceptionMessage);
        }
        if (nodesLength > 1) {
          log.warn(
              "{}: More than one enum configuration found for enum '{}' in {}. The last one was taken.",
              ftName,
              enumName,
              enumsConfigFile);
        }
        final Node enumNode = nodes.item(nodesLength - 1);
        final NodeList valueNodes =
            (NodeList) xpath.evaluate("value", enumNode, XPathConstants.NODESET);
        for (int i = 0; i < valueNodes.getLength(); i++) {
          final Node valueNode = valueNodes.item(i);
          final String valueStr = valueNode.getTextContent();
          if ((valueStr == null) || (valueStr.length() == 0)) {
            final String exceptionMessage =
                String.format(
                    Locale.ENGLISH,
                    "%s: A value was defined with an no value in enum '%s' in %s.",
                    ftName,
                    enumName,
                    enumsConfigFile);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMessage);
          }
          if (enumStringToIntMap.containsKey(valueStr)) {
            final String exceptionMessage =
                String.format(
                    Locale.ENGLISH,
                    "%s: A duplicated definition was found for value '%s' in enum '%s' in %s.",
                    ftName,
                    valueStr,
                    enumName,
                    enumsConfigFile);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMessage);
          }
          enumIntToStringMap.put(i, valueStr);
          enumStringToIntMap.put(valueStr, i);
        }
      } catch (IOException | SAXException | XPathExpressionException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, ftName + ": Error while parsing enums config.", e);
      }

      if ((enumStringToIntMap.size() == 0) || (enumIntToStringMap.size() == 0)) {
        String exceptionMessage =
            String.format(
                Locale.ENGLISH,
                "%s: Invalid configuration was defined for enum '%s' in %s.",
                ftName,
                enumName,
                enumsConfigFile);
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, exceptionMessage);
      }

      this.enumStringToIntMap = Collections.unmodifiableMap(enumStringToIntMap);
      this.enumIntToStringMap = Collections.unmodifiableMap(enumIntToStringMap);

      args.remove(PARAM_ENUMS_CONFIG);
      args.remove(PARAM_ENUM_NAME);
    }

    /**
     * Converting the (internal) integer value (indicating the sort order) to string (displayed)
     * value
     *
     * @param intVal integer value
     * @return string value
     */
    public String intValueToStringValue(Integer intVal) {
      if (intVal == null) return null;

      final String enumString = enumIntToStringMap.get(intVal);
      if (enumString != null) return enumString;
      // can't find matching enum name - return DEFAULT_VALUE.toString()
      return DEFAULT_VALUE.toString();
    }

    /**
     * Converting the string (displayed) value (internal) to integer value (indicating the sort
     * order)
     *
     * @param stringVal string value
     * @return integer value
     */
    public Integer stringValueToIntValue(String stringVal) {
      if (stringVal == null) return null;

      Integer intValue;
      final Integer enumInt = enumStringToIntMap.get(stringVal);
      if (enumInt != null) { // enum int found for string
        return enumInt;
      }

      // enum int not found for string
      intValue = tryParseInt(stringVal);
      if (intValue == null) { // not Integer
        intValue = DEFAULT_VALUE;
      }
      final String enumString = enumIntToStringMap.get(intValue);
      if (enumString != null) { // has matching string
        return intValue;
      }

      return DEFAULT_VALUE;
    }

    private static Integer tryParseInt(String valueStr) {
      Integer intValue = null;
      try {
        intValue = Integer.parseInt(valueStr);
      } catch (NumberFormatException ignore) {
      }
      return intValue;
    }
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  protected Query getSpecializedRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    Integer minValue = enumMapping.stringValueToIntValue(min);
    Integer maxValue = enumMapping.stringValueToIntValue(max);

    if (field.indexed()) {
      BytesRef minBytes = null;
      if (min != null) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(minValue, bytes, 0);
        minBytes = new BytesRef(bytes);
      }
      BytesRef maxBytes = null;
      if (max != null) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(maxValue, bytes, 0);
        maxBytes = new BytesRef(bytes);
      }
      return new TermRangeQuery(field.getName(), minBytes, maxBytes, minInclusive, maxInclusive);

    } else {
      long lowerValue = Long.MIN_VALUE;
      long upperValue = Long.MAX_VALUE;
      if (minValue != null) {
        lowerValue = minValue.longValue();
        if (minInclusive == false) {
          ++lowerValue;
        }
      }
      if (maxValue != null) {
        upperValue = maxValue.longValue();
        if (maxInclusive == false) {
          --upperValue;
        }
      }
      if (field.multiValued()) {
        return new ConstantScoreQuery(
            SortedNumericDocValuesField.newSlowRangeQuery(field.getName(), lowerValue, upperValue));
      } else {
        return new ConstantScoreQuery(
            NumericDocValuesField.newSlowRangeQuery(field.getName(), lowerValue, upperValue));
      }
    }
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    final String s = val.toString();
    if (s == null) return;

    result.grow(Integer.BYTES);
    result.setLength(Integer.BYTES);
    final Integer intValue = enumMapping.stringValueToIntValue(s);
    NumericUtils.intToSortableBytes(intValue, result.bytes(), 0);
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    if (indexedForm == null) return null;
    final BytesRef bytesRef = new BytesRef(indexedForm);
    final Integer intValue = NumericUtils.sortableBytesToInt(bytesRef.bytes, 0);
    return enumMapping.intValueToStringValue(intValue);
  }

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    final Integer intValue = NumericUtils.sortableBytesToInt(input.bytes, 0);
    final String stringValue = enumMapping.intValueToStringValue(intValue);
    output.grow(stringValue.length());
    output.setLength(stringValue.length());
    stringValue.getChars(0, output.length(), output.chars(), 0);
    return output.get();
  }

  @Override
  public EnumFieldValue toObject(SchemaField sf, BytesRef term) {
    final Integer intValue = NumericUtils.sortableBytesToInt(term.bytes, 0);
    final String stringValue = enumMapping.intValueToStringValue(intValue);
    return new EnumFieldValue(intValue, stringValue);
  }

  @Override
  public EnumFieldValue toObject(IndexableField f) {
    Integer intValue = null;
    String stringValue = null;
    final Number val = f.numericValue();
    if (val != null) {
      intValue = val.intValue();
      stringValue = enumMapping.intValueToStringValue(intValue);
    }
    return new EnumFieldValue(intValue, stringValue);
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null) return null;
    final BytesRefBuilder bytes = new BytesRefBuilder();
    bytes.grow(Integer.BYTES);
    bytes.setLength(Integer.BYTES);
    NumericUtils.intToSortableBytes(val.intValue(), bytes.bytes(), 0);
    return bytes.get().utf8ToString();
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    final Integer intValue = enumMapping.stringValueToIntValue(value.toString());
    if (intValue == null || intValue.equals(EnumMapping.DEFAULT_VALUE)) {
      String exceptionMessage =
          String.format(
              Locale.ENGLISH,
              "Unknown value for enum field: %s, value: %s",
              field.getName(),
              value.toString());
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exceptionMessage);
    }

    org.apache.lucene.document.FieldType newType = new org.apache.lucene.document.FieldType();
    newType.setTokenized(false);
    newType.setStored(field.stored());
    newType.setOmitNorms(field.omitNorms());
    newType.setIndexOptions(field.indexOptions());
    newType.setStoreTermVectors(field.storeTermVector());
    newType.setStoreTermVectorOffsets(field.storeTermOffsets());
    newType.setStoreTermVectorPositions(field.storeTermPositions());
    newType.setStoreTermVectorPayloads(field.storeTermPayloads());

    byte[] bytes = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(intValue, bytes, 0);
    return new Field(field.getName(), bytes, newType) {
      @Override
      public Number numericValue() {
        return NumericUtils.sortableBytesToInt(((BytesRef) fieldsData).bytes, 0);
      }

      @Override
      public StoredValue storedValue() {
        return new StoredValue(NumericUtils.sortableBytesToInt(((BytesRef) fieldsData).bytes, 0));
      }
    };
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    if (!sf.hasDocValues()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " requires docValues=\"true\".");
    }
    final IndexableField field = createField(sf, value);
    final List<IndexableField> fields = new ArrayList<>();
    fields.add(field);
    final long longValue = field.numericValue().longValue();
    if (sf.multiValued()) {
      fields.add(new SortedNumericDocValuesField(sf.getName(), longValue));
    } else {
      fields.add(new NumericDocValuesField(sf.getName(), longValue));
    }
    return fields;
  }

  @Override
  public final ValueSource getSingleValueSource(
      MultiValueSelector choice, SchemaField field, QParser parser) {
    if (!field.multiValued()) { // trivial base case
      return getValueSource(field, parser); // single value matches any selector
    }
    SortedNumericSelector.Type selectorType = choice.getSortedNumericSelectorType();
    if (null == selectorType) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          choice.toString()
              + " is not a supported option for picking a single value"
              + " from the multivalued field: "
              + field.getName()
              + " (type: "
              + this.getTypeName()
              + ")");
    }
    return new MultiValuedIntFieldSource(field.getName(), selectorType);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    final SortField result = getNumericSort(field, NumberType.INTEGER, top);
    if (null == result.getMissingValue()) {
      // special case 'enum' default behavior: assume missing values are "below" all enum values
      result.setMissingValue(Integer.MIN_VALUE);
    }
    return result;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new EnumFieldSource(
        field.getName(), enumMapping.enumIntToStringMap, enumMapping.enumStringToIntMap);
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    final Number val = f.numericValue();
    if (val == null) {
      writer.writeNull(name);
      return;
    }

    final String readableValue = enumMapping.intValueToStringValue(val.intValue());
    writer.writeStr(name, readableValue, true);
  }

  @Override
  public boolean isTokenized() {
    return false;
  }

  @Override
  public NumberType getNumberType() {
    return NumberType.INTEGER;
  }

  @Override
  public String readableToIndexed(String val) {
    if (val == null) return null;

    final BytesRefBuilder bytes = new BytesRefBuilder();
    readableToIndexed(val, bytes);
    return bytes.get().utf8ToString();
  }

  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }

  @Override
  public String toExternal(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null) return null;

    return enumMapping.intValueToStringValue(val.intValue());
  }

  @Override
  public Object toNativeType(Object val) {
    if (val instanceof CharSequence) {
      final String str = val.toString();
      final Integer entry = enumMapping.enumStringToIntMap.get(str);
      if (entry != null) {
        return new EnumFieldValue(entry, str);
      } else {
        try {
          final int num = Integer.parseInt(str);
          return new EnumFieldValue(num, enumMapping.enumIntToStringMap.get(num));
        } catch (NumberFormatException ignore) {
          // Could not convert to Integer so fall through.
        }
      }
    } else if (val instanceof Number) {
      final int num = ((Number) val).intValue();
      return new EnumFieldValue(num, enumMapping.enumIntToStringMap.get(num));
    }

    return super.toNativeType(val);
  }
}
