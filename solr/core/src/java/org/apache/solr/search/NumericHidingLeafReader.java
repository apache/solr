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
package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.uninverting.UninvertingReader;

/**
 * This class is useful in two specific situations:
 *
 * <ul>
 *   <li>calling {@code ord} or {@code rord} functions on a single-valued numeric field.
 *   <li>doing grouped faceting ({@code group.facet}) on a single-valued numeric field.
 * </ul>
 */
@Deprecated
public class NumericHidingLeafReader extends FilterLeafReader {

  /**
   * Returns a view over {@code leafReader} where {@code field} is a string instead of a numeric.
   */
  public static LeafReader wrap(LeafReader leafReader, String field) {
    return UninvertingReader.wrap(
        new NumericHidingLeafReader(leafReader, field),
        Collections.singletonMap(field, UninvertingReader.Type.SORTED)::get);
  }

  private final String field;
  private final FieldInfos fieldInfos;

  private NumericHidingLeafReader(LeafReader in, String field) {
    super(in);
    this.field = field;
    ArrayList<FieldInfo> filteredInfos = new ArrayList<>();
    for (FieldInfo fi : in.getFieldInfos()) {
      if (fi.name.equals(field)) {
        filteredInfos.add(
            new FieldInfo(
                fi.name,
                fi.number,
                fi.hasVectors(),
                fi.omitsNorms(),
                fi.hasPayloads(),
                fi.getIndexOptions(),
                DocValuesType.NONE,
                -1,
                Collections.emptyMap(),
                fi.getPointDimensionCount(),
                fi.getPointIndexDimensionCount(),
                fi.getPointNumBytes(),
                fi.getVectorDimension(),
                fi.getVectorEncoding(),
                fi.getVectorSimilarityFunction(),
                fi.isSoftDeletesField()));
      } else {
        filteredInfos.add(fi);
      }
    }
    fieldInfos = new FieldInfos(filteredInfos.toArray(new FieldInfo[filteredInfos.size()]));
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    if (this.field.equals(field)) {
      return null;
    } else {
      return in.getNumericDocValues(field);
    }
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    if (this.field.equals(field)) {
      return null;
    } else {
      return in.getBinaryDocValues(field);
    }
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    if (this.field.equals(field)) {
      return null;
    } else {
      return in.getSortedDocValues(field);
    }
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    if (this.field.equals(field)) {
      return null;
    } else {
      return in.getSortedSetDocValues(field);
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  // important to override these, so fieldcaches are shared on what we wrap

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }
}
