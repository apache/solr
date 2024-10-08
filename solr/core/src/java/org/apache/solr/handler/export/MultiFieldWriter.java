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

package org.apache.solr.handler.export;

import java.io.IOException;
import java.util.Date;
import java.util.function.LongFunction;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocValuesIteratorCache;

class MultiFieldWriter extends FieldWriter {
  private final String field;
  private final FieldType fieldType;
  private final SchemaField schemaField;
  private final boolean numeric;
  private final CharsRefBuilder cref = new CharsRefBuilder();
  private final LongFunction<Object> bitsToValue;
  private final DocValuesIteratorCache.FieldDocValuesSupplier docValuesCache;

  public MultiFieldWriter(
      String field,
      FieldType fieldType,
      SchemaField schemaField,
      boolean numeric,
      DocValuesIteratorCache.FieldDocValuesSupplier docValuesCache) {
    this.field = field;
    this.fieldType = fieldType;
    this.schemaField = schemaField;
    this.numeric = numeric;
    if (this.fieldType.isPointField()) {
      bitsToValue = bitsToValue(fieldType);
    } else {
      bitsToValue = null;
    }
    this.docValuesCache = docValuesCache;
  }

  @Override
  public boolean write(
      SortDoc sortDoc, LeafReaderContext readerContext, MapWriter.EntryWriter out, int fieldIndex)
      throws IOException {
    if (this.fieldType.isPointField()) {
      SortedNumericDocValues vals =
          docValuesCache.getSortedNumericDocValues(
              sortDoc.docId, readerContext.reader(), readerContext.ord);
      if (vals == null) {
        return false;
      }

      final SortedNumericDocValues docVals = vals;

      out.put(
          this.field,
          (IteratorWriter)
              w -> {
                for (int i = 0, count = docVals.docValueCount(); i < count; i++) {
                  w.add(bitsToValue.apply(docVals.nextValue()));
                }
              });
      return true;
    } else {
      SortedSetDocValues vals =
          docValuesCache.getSortedSetDocValues(
              sortDoc.docId, readerContext.reader(), readerContext.ord);
      if (vals == null) {
        return false;
      }

      final SortedSetDocValues docVals = vals;

      out.put(
          this.field,
          (IteratorWriter)
              w -> {
                for (int i = 0, count = docVals.docValueCount(); i < count; i++) {
                  BytesRef ref = docVals.lookupOrd(docVals.nextOrd());
                  fieldType.indexedToReadable(ref, cref);
                  IndexableField f = fieldType.createField(schemaField, cref.toString());
                  if (f == null) w.add(cref.toString());
                  else w.add(fieldType.toObject(f));
                }
              });
      return true;
    }
  }

  static LongFunction<Object> bitsToValue(FieldType fieldType) {
    switch (fieldType.getNumberType()) {
      case LONG:
        return (bits) -> bits;
      case DATE:
        return (bits) -> new Date(bits);
      case INTEGER:
        return (bits) -> (int) bits;
      case FLOAT:
        return (bits) -> NumericUtils.sortableIntToFloat((int) bits);
      case DOUBLE:
        return (bits) -> NumericUtils.sortableLongToDouble(bits);
      default:
        throw new AssertionError("Unsupported NumberType: " + fieldType.getNumberType());
    }
  }
}
