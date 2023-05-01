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
package org.apache.solr.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackingDirectoryReader extends FilterDirectoryReader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TrackingSubReaderWrapper trackingSubReaderWrapper;

  public TrackingDirectoryReader(
      DirectoryReader in, TrackingSubReaderWrapper trackingSubReaderWrapper) throws IOException {
    super(in, trackingSubReaderWrapper);
    this.trackingSubReaderWrapper = trackingSubReaderWrapper;
  }

  public static DirectoryReader wrap(DirectoryReader in) throws IOException {
    return new TrackingDirectoryReader(in, new TrackingSubReaderWrapper());
  }

  public Map<Map.Entry<String, String>, LongAdder> getUsage() {
    return Collections.unmodifiableMap(this.trackingSubReaderWrapper.fieldUsage);
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return wrap(in);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  private static class TrackingSubReaderWrapper extends SubReaderWrapper {
    private final Map<Map.Entry<String, String>, LongAdder> fieldUsage = new ConcurrentHashMap<>();

    @Override
    public LeafReader wrap(LeafReader reader) {
      return new TrackingFilterLeafReader(reader);
    }

    private class TrackingFilterLeafReader extends FilterLeafReader {
      private static final String POINT_TYPE = "point";
      private static final String FLOAT_VECTOR_TYPE = "floatvector";
      private static final String BYTE_VECTOR_TYPE = "bytevector";
      private static final String SEARCH_FLOAT_NN_TYPE = "searchfloatnn";
      private static final String SEARCH_BYTE_NN_TYPE = "searchbytenn";
      private static final String TERMS_TYPE = "terms";
      private static final String NUMERIC_DOCVALUES_TYPE = "numericdocvalues";
      private static final String BINARY_DOCVALUES_TYPE = "binarydocvalues";
      private static final String SORTED_DOCVALUES_TYPE = "sorteddocvalues";
      private static final String SORTED_NUMERIC_DOCVALUES_TYPE = "sortednumericdocvalues";
      private static final String SORTED_SET_DOCVALUES_TYPE = "sortedsetdocvalues";
      private static final String NORMS_TYPE = "norms";

      private TrackingFilterLeafReader(LeafReader in) {
        super(in);
      }

      private void trackUsage(String field, String type) {
        final Map.Entry<String, String> key = new AbstractMap.SimpleImmutableEntry<>(field, type);
        fieldUsage.computeIfAbsent(key, k -> new LongAdder()).increment();
        if (log.isDebugEnabled()) {
          log.debug("{} {} usage is: {}", field, type, fieldUsage.get(key));
        }
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
      }

      @Override
      public PointValues getPointValues(String field) throws IOException {
        trackUsage(field, POINT_TYPE);
        return super.getPointValues(field);
      }

      @Override
      public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        trackUsage(field, FLOAT_VECTOR_TYPE);
        return super.getFloatVectorValues(field);
      }

      @Override
      public ByteVectorValues getByteVectorValues(String field) throws IOException {
        trackUsage(field, BYTE_VECTOR_TYPE);
        return super.getByteVectorValues(field);
      }

      @Override
      public TopDocs searchNearestVectors(
          String field, float[] target, int k, Bits acceptDocs, int visitedLimit)
          throws IOException {
        trackUsage(field, SEARCH_FLOAT_NN_TYPE);
        return super.searchNearestVectors(field, target, k, acceptDocs, visitedLimit);
      }

      @Override
      public TopDocs searchNearestVectors(
          String field, byte[] target, int k, Bits acceptDocs, int visitedLimit)
          throws IOException {
        trackUsage(field, SEARCH_BYTE_NN_TYPE);
        return super.searchNearestVectors(field, target, k, acceptDocs, visitedLimit);
      }

      @Override
      public Terms terms(String field) throws IOException {
        trackUsage(field, TERMS_TYPE);
        return super.terms(field);
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) throws IOException {
        trackUsage(field, NUMERIC_DOCVALUES_TYPE);
        return super.getNumericDocValues(field);
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        trackUsage(field, BINARY_DOCVALUES_TYPE);
        return super.getBinaryDocValues(field);
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        trackUsage(field, SORTED_DOCVALUES_TYPE);
        return super.getSortedDocValues(field);
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        trackUsage(field, SORTED_NUMERIC_DOCVALUES_TYPE);
        return super.getSortedNumericDocValues(field);
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        trackUsage(field, SORTED_SET_DOCVALUES_TYPE);
        return super.getSortedSetDocValues(field);
      }

      @Override
      public NumericDocValues getNormValues(String field) throws IOException {
        trackUsage(field, NORMS_TYPE);
        return super.getNormValues(field);
      }
    }
  }
}
