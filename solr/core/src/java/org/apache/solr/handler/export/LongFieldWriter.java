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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.common.MapWriter;
import org.apache.solr.search.DocValuesIteratorCache;

class LongFieldWriter extends FieldWriter {
  protected final String field;

  private final DocValuesIteratorCache.FieldDocValuesSupplier docValuesCache;

  public LongFieldWriter(
      String field, DocValuesIteratorCache.FieldDocValuesSupplier docValuesCache) {
    this.field = field;
    this.docValuesCache = docValuesCache;
  }

  @Override
  public boolean write(
      SortDoc sortDoc, LeafReaderContext readerContext, MapWriter.EntryWriter ew, int fieldIndex)
      throws IOException {
    long val;
    SortValue sortValue = sortDoc.getSortValue(this.field);
    if (sortValue != null) {
      if (sortValue.isPresent()) {
        val = (long) sortValue.getCurrentValue();
      } else { // empty-value
        return false;
      }
    } else {
      // field is not part of 'sort' param, but part of 'fl' param
      NumericDocValues vals =
          docValuesCache.getNumericDocValues(
              sortDoc.docId, readerContext.reader(), readerContext.ord);
      if (vals != null) {
        val = vals.longValue();
      } else {
        return false;
      }
    }
    doWrite(ew, val);
    return true;
  }

  protected void doWrite(MapWriter.EntryWriter ew, long val) throws IOException {
    ew.put(field, val);
  }
}
