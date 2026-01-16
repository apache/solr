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

  private final Map<String, SchemaField> fields;
  private final ThreadLocal<WeakHashMap<IndexReader.CacheKey, StoredFields>> storedFieldsMap =
      new ThreadLocal<>();

  public StoredFieldsWriter(Map<String, SchemaField> fieldsToRead) {
    this.fields = fieldsToRead;
  }

  @Override
  public int write(
      SortDoc sortDoc, LeafReaderContext readerContext, EntryWriter out, int fieldIndex)
      throws IOException {
    WeakHashMap<IndexReader.CacheKey, StoredFields> map = storedFieldsMap.get();
    if (map == null) {
      map = new WeakHashMap<>();
      storedFieldsMap.set(map);
    }
    LeafReader reader = readerContext.reader();
    StoredFields storedFields = map.get(reader.getReaderCacheHelper().getKey());
    if (storedFields == null) {
      storedFields = reader.storedFields();
      map.put(reader.getReaderCacheHelper().getKey(), storedFields);
    }
    ExportVisitor visitor = new ExportVisitor(out);
    storedFields.document(sortDoc.docId, visitor);
    return visitor.flush();
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
      var schemaField = fields.get(fieldInfo.name);
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
      var schemaField = fields.get(fieldInfo.name);
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
      return fields.containsKey(fieldInfo.name) ? Status.YES : Status.NO;
    }

    private <T> void addField(String fieldName, T value) throws IOException {
      if (fields.get(fieldName).multiValued()) {
        if (fieldName.equals(lastFieldName)) {
          multiValue.add(value);
        } else {
          if (multiValue != null) {
            out.put(lastFieldName, multiValue);
          }
          multiValue = new ArrayList<>();
          lastFieldName = fieldName;
          multiValue.add(value);
          fieldsVisited++;
        }
      } else {
        out.put(fieldName, value);
        fieldsVisited++;
      }
    }

    private int flush() throws IOException {
      if (lastFieldName != null && multiValue != null && !multiValue.isEmpty()) {
        out.put(lastFieldName, multiValue);
      }
      return fieldsVisited;
    }
  }
}
