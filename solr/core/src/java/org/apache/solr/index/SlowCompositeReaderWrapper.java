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
package org.apache.solr.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.packed.PackedInts;

/**
 * This class forces a composite reader (eg a {@link MultiReader} or {@link DirectoryReader}) to
 * emulate a {@link LeafReader}. This requires implementing the postings APIs on-the-fly, using the
 * static methods in {@link MultiTerms}, {@link MultiDocValues}, by stepping through the sub-readers
 * to merge fields/terms, appending docs, etc.
 *
 * <p><b>NOTE</b>: this class almost always results in a performance hit. If this is important to
 * your use case, you'll get better performance by gathering the sub readers using {@link
 * IndexReader#getContext()} to get the leaves and then operate per-LeafReader, instead of using
 * this class.
 */
public final class SlowCompositeReaderWrapper extends LeafReader {

  private final CompositeReader in;
  private final LeafMetaData metaData;

  // Cached copy of FieldInfos to prevent it from being re-created on each
  // getFieldInfos call.  Most (if not all) other LeafReader implementations
  // also have a cached FieldInfos instance so this is consistent. SOLR-12878
  private final FieldInfos fieldInfos;

  // TODO: this could really be a weak map somewhere else on the coreCacheKey,
  // but do we really need to optimize slow-wrapper any more?
  final Map<String, OrdinalMap> cachedOrdMaps = new ConcurrentHashMap<>();

  /**
   * This method is sugar for getting an {@link LeafReader} from an {@link IndexReader} of any kind.
   * If the reader is already atomic, it is returned unchanged, otherwise wrapped by this class.
   */
  public static LeafReader wrap(IndexReader reader) throws IOException {
    if (reader instanceof CompositeReader) {
      return new SlowCompositeReaderWrapper((CompositeReader) reader);
    } else {
      assert reader instanceof LeafReader;
      return (LeafReader) reader;
    }
  }

  SlowCompositeReaderWrapper(CompositeReader reader) throws IOException {
    in = reader;
    in.registerParentReader(this);
    if (reader.leaves().isEmpty()) {
      metaData = new LeafMetaData(Version.LATEST.major, Version.LATEST, null);
    } else {
      Version minVersion = Version.LATEST;
      for (LeafReaderContext leafReaderContext : reader.leaves()) {
        Version leafVersion = leafReaderContext.reader().getMetaData().getMinVersion();
        if (leafVersion == null) {
          minVersion = null;
          break;
        } else if (minVersion.onOrAfter(leafVersion)) {
          minVersion = leafVersion;
        }
      }
      int createdVersionMajor =
          reader.leaves().get(0).reader().getMetaData().getCreatedVersionMajor();
      metaData = new LeafMetaData(createdVersionMajor, minVersion, null);
    }
    fieldInfos = FieldInfos.getMergedFieldInfos(in);
  }

  @Override
  public String toString() {
    return "SlowCompositeReaderWrapper(" + in + ")";
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    // TODO: this is trappy as the expectation is that core keys live for a long
    // time, but here we need to bound it to the lifetime of the wrapped
    // composite reader? Unfortunately some features seem to rely on this...
    return in.getReaderCacheHelper();
  }

  @Override
  public Terms terms(String field) throws IOException {
    ensureOpen();
    return MultiTerms.getTerms(in, field);
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getNumericValues(in, field); // TODO cache?
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getBinaryValues(in, field); // TODO cache?
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getSortedNumericValues(in, field); // TODO cache?
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();

    // Integration of what was previously in MultiDocValues.getSortedValues:
    // The purpose of this integration is to be able to construct a value producer which can always
    // produce a value that is actually needed. The reason for the producer is to avoid getAndSet
    // pitfalls in this multithreaded context.
    // So all cases that do not lead to a cacheable value are handled upfront.
    // We kept the semantics of MultiDocValues.getSortedValues.
    final List<LeafReaderContext> leaves = in.leaves();
    final int size = leaves.size();

    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedDocValues(field);
    }

    boolean anyReal = false;
    final SortedDocValues[] values = new SortedDocValues[size];
    final int[] starts = new int[size + 1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = in.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED) {
        return null;
      }
      SortedDocValues v = reader.getSortedDocValues(field);
      if (v == null) {
        v = DocValues.emptySorted();
      } else {
        anyReal = true;
      }
      totalCost += v.cost();
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = maxDoc();
    if (anyReal == false) {
      return null;
    }

    // at this point in time we are able to formulate the producer
    OrdinalMap map = null;
    CacheHelper cacheHelper = getReaderCacheHelper();

    Function<? super String, ? extends OrdinalMap> producer =
        (notUsed) -> {
          try {
            OrdinalMap mapping =
                OrdinalMap.build(
                    cacheHelper == null ? null : cacheHelper.getKey(), values, PackedInts.DEFAULT);
            return mapping;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };

    // either we use a cached result that gets produced eventually during caching,
    // or we produce directly without caching
    if (cacheHelper != null) {
      map = cachedOrdMaps.computeIfAbsent(field + cacheHelper.getKey(), producer);
    } else {
      map = producer.apply("notUsed");
    }

    return new MultiSortedDocValues(values, starts, map, totalCost);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();

    // Integration of what was previously in MultiDocValues.getSortedSetValues:
    // The purpose of this integration is to be able to construct a value producer which can always
    // produce a value that is actually needed. The reason for the producer is to avoid getAndSet
    // pitfalls in this multithreaded context.
    // So all cases that do not lead to a cacheable value are handled upfront.
    // We kept the semantics of MultiDocValues.getSortedSetValues.
    final List<LeafReaderContext> leaves = in.leaves();
    final int size = leaves.size();

    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getSortedSetDocValues(field);
    }

    boolean anyReal = false;
    final SortedSetDocValues[] values = new SortedSetDocValues[size];
    final int[] starts = new int[size + 1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = in.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET) {
        return null;
      }
      SortedSetDocValues v = reader.getSortedSetDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedSet();
      } else {
        anyReal = true;
      }
      totalCost += v.cost();
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = maxDoc();
    if (anyReal == false) {
      return null;
    }

    // at this point in time we are able to formulate the producer
    OrdinalMap map = null;
    CacheHelper cacheHelper = getReaderCacheHelper();

    Function<? super String, ? extends OrdinalMap> producer =
        (notUsed) -> {
          try {
            OrdinalMap mapping =
                OrdinalMap.build(
                    cacheHelper == null ? null : cacheHelper.getKey(), values, PackedInts.DEFAULT);
            return mapping;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };

    // either we use a cached result that gets produced eventually during caching,
    // or we produce directly without caching
    if (cacheHelper != null) {
      map = cachedOrdMaps.computeIfAbsent(field + cacheHelper.getKey(), producer);
    } else {
      map = producer.apply("notUsed");
    }

    return new MultiDocValues.MultiSortedSetDocValues(values, starts, map, totalCost);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getNormValues(in, field); // TODO cache?
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    return in.getTermVectors(docID);
  }

  @Override
  public TermVectors termVectors() throws IOException {
    ensureOpen();
    return in.termVectors();
  }

  @Override
  public StoredFields storedFields() throws IOException {
    ensureOpen();
    return in.storedFields();
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return MultiBits.getLiveDocs(in); // TODO cache?
  }

  @Override
  public PointValues getPointValues(String field) {
    ensureOpen();
    throw new UnsupportedOperationException();
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) {
    ensureOpen();
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    ensureOpen();
    throw new UnsupportedOperationException();
  }

  @Override
  public void searchNearestVectors(
      String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void searchNearestVectors(
      String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  protected void doClose() throws IOException {
    // TODO: as this is a wrapper, should we really close the delegate?
    in.close();
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    for (LeafReaderContext ctx : in.leaves()) {
      ctx.reader().checkIntegrity();
    }
  }

  @Override
  public LeafMetaData getMetaData() {
    return metaData;
  }
}
