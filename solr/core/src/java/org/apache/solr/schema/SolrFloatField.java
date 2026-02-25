package org.apache.solr.schema;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public final class SolrFloatField extends Field {

  static org.apache.lucene.document.FieldType getType(
      boolean rangeIndex, boolean termIndex, boolean docValues, boolean stored) {
    org.apache.lucene.document.FieldType type = new org.apache.lucene.document.FieldType();
    if (rangeIndex) {
      type.setDimensions(1, Float.BYTES);
    }
    if (termIndex) {
      type.setIndexOptions(IndexOptions.DOCS);
    }
    if (docValues) {
      type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    }
    type.setTokenized(false);
    type.setStored(stored);
    type.freeze();
    return type;
  }

  private final StoredValue storedValue;

  /**
   * Creates a new FloatField, indexing the provided point, storing it as a DocValue, and optionally
   * storing it as a stored field.
   *
   * @param name field name
   * @param value the float value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public SolrFloatField(
      String name,
      float value,
      boolean rangeIndex,
      boolean termIndex,
      boolean docValues,
      boolean stored) {
    super(name, getType(rangeIndex, termIndex, docValues, stored));
    fieldsData = (long) NumericUtils.floatToSortableInt(value);
    if (stored) {
      storedValue = new StoredValue(value);
    } else {
      storedValue = null;
    }
  }

  @Override
  public InvertableType invertableType() {
    return InvertableType.BINARY;
  }

  @Override
  public BytesRef binaryValue() {
    byte[] encodedPoint = new byte[Float.BYTES];
    float value = getValueAsFloat();
    FloatPoint.encodeDimension(value, encodedPoint, 0);
    return new BytesRef(encodedPoint);
  }

  private float getValueAsFloat() {
    return NumericUtils.sortableIntToFloat(numericValue().intValue());
  }

  @Override
  public StoredValue storedValue() {
    return storedValue;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " <" + name + ':' + getValueAsFloat() + '>';
  }

  @Override
  public void setFloatValue(float value) {
    super.setLongValue(NumericUtils.floatToSortableInt(value));
    if (storedValue != null) {
      storedValue.setFloatValue(value);
    }
  }

  @Override
  public void setLongValue(long value) {
    throw new IllegalArgumentException("cannot change value type from Float to Long");
  }
}
