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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DateValueFieldType;
import org.apache.solr.schema.SchemaField;

class StoredFieldsWriter extends FieldWriter {

  private final Map<String, SchemaField> schemaFields;
  private static final ThreadLocal<WeakHashMap<IndexReader.CacheKey, StoredFields>>
      STORED_FIELDS_MAP = ThreadLocal.withInitial(WeakHashMap::new);

  public StoredFieldsWriter(Map<String, SchemaField> fieldsToRead) {
    this.schemaFields = fieldsToRead;
  }

  @Override
  public void write(SortDoc sortDoc, LeafReaderContext readerContext, EntryWriter out)
      throws IOException {
    WeakHashMap<IndexReader.CacheKey, StoredFields> map = STORED_FIELDS_MAP.get();
    LeafReader reader = readerContext.reader();
    StoredFields storedFields = map.get(reader.getReaderCacheHelper().getKey());
    if (storedFields == null) {
      storedFields = reader.storedFields();
      map.put(reader.getReaderCacheHelper().getKey(), storedFields);
    }
    ExportVisitor visitor = new ExportVisitor(out);
    storedFields.document(sortDoc.docId, visitor);
    visitor.flush();
  }

  class ExportVisitor extends StoredFieldVisitor {

    final EntryWriter out;
    String lastFieldName;
    List<Object> multiValue = null;
    int fieldsVisited;

    public ExportVisitor(EntryWriter out) {
      this.out = out;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      var schemaField = schemaFields.get(fieldInfo.name);
      var fieldType = schemaField == null ? null : schemaField.getType();
      if (fieldType instanceof BoolField) {
        // Convert "T"/"F" stored value to boolean true/false
        addField(fieldInfo.name, Boolean.valueOf(fieldType.indexedToReadable(value)));
      } else {
        addField(fieldInfo.name, value);
      }
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      addField(fieldInfo.name, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      var schemaField = schemaFields.get(fieldInfo.name);
      var fieldType = schemaField == null ? null : schemaField.getType();
      if (fieldType instanceof DateValueFieldType) {
        Date date = new Date(value);
        addField(fieldInfo.name, date);
      } else {
        addField(fieldInfo.name, value);
      }
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      addField(fieldInfo.name, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      addField(fieldInfo.name, value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
      return schemaFields.containsKey(fieldInfo.name) ? Status.YES : Status.NO;
    }

    private <T> void addField(String fieldName, T value) throws IOException {
      if (fieldName.equals(lastFieldName)) {
        // assume adding another value to a multi-value field
        multiValue.add(value);
        return;
      }
      // new/different field...
      flush(); // completes the previous field if there's something to do
      fieldsVisited++;
      lastFieldName = fieldName;

      if (schemaFields.get(fieldName).multiValued()) {
        multiValue = new ArrayList<>();
        multiValue.add(value);
      } else {
        out.put(fieldName, value);
      }
    }

    private void flush() throws IOException {
      if (multiValue != null) {
        out.put(lastFieldName, multiValue);
        multiValue = null;
      }
    }
  }
}
