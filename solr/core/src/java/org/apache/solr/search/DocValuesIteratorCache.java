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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.function.Function;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.schema.SchemaField;

/**
 * A helper class for random-order value access over docValues (such as in the case of
 * useDocValuesAsStored). This class optimizes access by reusing DocValues iterators where possible,
 * and by narrowing the scope of DocValues per-field/per-segment (shortcircuiting attempts to
 * `advance()` to docs that are known to have no value for a given field).
 */
public class DocValuesIteratorCache {

  private static final EnumMap<DocValuesType, IOBiFunction<LeafReader, String, DocIdSetIterator>>
      funcMap = new EnumMap<>(DocValuesType.class);

  static {
    funcMap.put(DocValuesType.NUMERIC, LeafReader::getNumericDocValues);
    funcMap.put(DocValuesType.BINARY, LeafReader::getBinaryDocValues);
    funcMap.put(
        DocValuesType.SORTED,
        (r, f) -> {
          SortedDocValues dvs = r.getSortedDocValues(f);
          return dvs == null || dvs.getValueCount() < 1 ? null : dvs;
        });
    funcMap.put(DocValuesType.SORTED_NUMERIC, LeafReader::getSortedNumericDocValues);
    funcMap.put(
        DocValuesType.SORTED_SET,
        (r, f) -> {
          SortedSetDocValues dvs = r.getSortedSetDocValues(f);
          return dvs == null || dvs.getValueCount() < 1 ? null : dvs;
        });
  }

  private static final FieldDocValuesSupplier NONE = new FieldDocValuesSupplier(null, null, 0);

  private final SolrIndexSearcher searcher;
  private final int nLeaves;
  private final Function<String, FieldDocValuesSupplier> getSupplier;

  /**
   * Construct an instance used to optimize random-order DocValues iterator access for the specified
   * searcher.
   */
  public DocValuesIteratorCache(SolrIndexSearcher searcher) {
    this(searcher, true);
  }

  /**
   * Construct an instance used to optimize random-order DocValues iterator access for the specified
   * searcher.
   *
   * @param searcher the associated searcher
   * @param cache if false, caching is disabled (useful mainly for single-field, single-doc access).
   */
  public DocValuesIteratorCache(SolrIndexSearcher searcher, boolean cache) {
    this.searcher = searcher;
    this.nLeaves = searcher.getTopReaderContext().leaves().size();
    if (cache) {
      HashMap<String, FieldDocValuesSupplier> map = new HashMap<>();
      getSupplier = (f) -> map.computeIfAbsent(f, this::newEntry);
    } else {
      getSupplier = this::newEntry;
    }
  }

  public FieldDocValuesSupplier getSupplier(String fieldName) {
    FieldDocValuesSupplier ret = getSupplier.apply(fieldName);
    return ret == NONE ? null : ret;
  }

  private FieldDocValuesSupplier newEntry(String fieldName) {
    final SchemaField schemaField = searcher.getSchema().getFieldOrNull(fieldName);
    FieldInfo fi = searcher.getFieldInfos().fieldInfo(fieldName);
    if (schemaField == null || !schemaField.hasDocValues() || fi == null) {
      return NONE; // Searcher doesn't have info about this field, hence ignore it.
    }
    final DocValuesType dvType = fi.getDocValuesType();
    switch (dvType) {
      case NUMERIC:
      case BINARY:
      case SORTED:
      case SORTED_NUMERIC:
      case SORTED_SET:
        return new FieldDocValuesSupplier(schemaField, dvType, nLeaves);
      default:
        return NONE;
    }
  }

  private interface IOBiFunction<T, U, R> {
    R apply(T t, U u) throws IOException;
  }

  /**
   * Supplies (and coordinates arbitrary-order value retrieval over) docValues iterators for a
   * particular field, encapsulating the logic of iterator creation, reuse/caching, and advancing.
   * Returned iterators are already positioned, and should <i>not</i> be advanced (though
   * multi-valued iterators may consume/iterate over values/ords).
   *
   * <p>Instances of this class are specifically designed to support arbitrary-order value
   * retrieval, (e.g., useDocValuesAsStored, ExportWriter) and should generally not be used for
   * ordered retrieval (although ordered retrieval would work perfectly fine, and would add only
   * minimal overhead).
   */
  public static class FieldDocValuesSupplier {
    public final SchemaField schemaField;
    public final DocValuesType type;
    private final int[] minLocalIds;
    private final int[] ceilingIds;
    private final int[] noMatchSince;
    private final DocIdSetIterator[] perLeaf;

    private FieldDocValuesSupplier(SchemaField schemaField, DocValuesType type, int nLeaves) {
      this.schemaField = schemaField;
      this.type = type;
      this.minLocalIds = new int[nLeaves];
      Arrays.fill(minLocalIds, -1);
      this.ceilingIds = new int[nLeaves];
      Arrays.fill(ceilingIds, DocIdSetIterator.NO_MORE_DOCS);
      this.noMatchSince = new int[nLeaves];
      this.perLeaf = new DocIdSetIterator[nLeaves];
    }

    /**
     * This method does the actual work caching iterators, determining eligibility for re-use,
     * pulling new iterators if necessary, and determining if we have a hit for a particular doc id.
     */
    private DocIdSetIterator getDocValues(
        int localId,
        LeafReader leafReader,
        int leafOrd,
        boolean singleValued,
        IOBiFunction<LeafReader, String, DocIdSetIterator> dvFunction)
        throws IOException {
      int min = minLocalIds[leafOrd];
      DocIdSetIterator dv;
      if (min == -1) {
        // we are not yet initialized for this field/leaf.
        dv = dvFunction.apply(leafReader, schemaField.getName());
        if (dv == null) {
          minLocalIds[leafOrd] = DocIdSetIterator.NO_MORE_DOCS; // cache absence of this field
          return null;
        }
        // on field/leaf init, determine the min doc, so that we don't expend effort pulling
        // new iterators for docs that fall below this floor.
        min = dv.nextDoc();
        minLocalIds[leafOrd] = min;
        perLeaf[leafOrd] = dv;
        if (localId < min) {
          noMatchSince[leafOrd] = 0; // implicit in initial `nextDoc()` call
          return null;
        } else if (localId == min) {
          noMatchSince[leafOrd] = DocIdSetIterator.NO_MORE_DOCS;
          return dv;
        }
      } else if (localId < min || localId >= ceilingIds[leafOrd]) {
        // out of range: either too low or too high
        return null;
      } else {
        dv = perLeaf[leafOrd];
        int currentDoc = dv.docID();
        if (localId == currentDoc) {
          if (singleValued) {
            return dv;
          } else if (noMatchSince[leafOrd] != DocIdSetIterator.NO_MORE_DOCS) {
            // `noMatchSince[leafOrd] != DocIdSetIterator.NO_MORE_DOCS` means that `dv` has not
            // been returned at its current position, and has therefore not been consumed and
            // is thus eligible to be returned directly. (singleValued dv iterators are always
            // eligible to be returned directly, as they have no concept of being "consumed")

            // NOTE: we must reset `noMatchSince[leafOrd]` here in order to prevent returning
            // consumed docValues; even though this actually loses us possible skipping information,
            // it's an edge case, and allows us to use `noMatchSince[leafOrd]` as a signal of
            // whether we have consumed multivalued docValues.
            noMatchSince[leafOrd] = DocIdSetIterator.NO_MORE_DOCS;
            return dv;
          }
        }
        if (localId <= currentDoc) {
          if (localId >= noMatchSince[leafOrd]) {
            // if the requested doc falls between the last requested doc and the current
            // position, then we know there's no match.
            return null;
          }
          // we must re-init the iterator
          dv = dvFunction.apply(leafReader, schemaField.getName());
          perLeaf[leafOrd] = dv;
        }
      }
      // NOTE: use `advance()`, not `advanceExact()`. There's no cost (in terms of re-use) to
      // doing so, because we track `noMatchSince` in the event of a miss.
      int found = dv.advance(localId);
      if (found == localId) {
        noMatchSince[leafOrd] = DocIdSetIterator.NO_MORE_DOCS;
        return dv;
      } else {
        if (found == DocIdSetIterator.NO_MORE_DOCS) {
          ceilingIds[leafOrd] = Math.min(localId, ceilingIds[leafOrd]);
        }
        noMatchSince[leafOrd] = localId;
        return null;
      }
    }

    /**
     * Returns docValues for the specified doc id in the specified reader, if the specified doc
     * holds docValues for this {@link FieldDocValuesSupplier} instance, otherwise returns null.
     *
     * <p>If a non-null value is returned, it will already positioned at the specified docId.
     *
     * @param localId leaf-scoped docId
     * @param leafReader reader containing docId
     * @param leafOrd top-level ord of the specified reader
     */
    public NumericDocValues getNumericDocValues(int localId, LeafReader leafReader, int leafOrd)
        throws IOException {
      return (NumericDocValues)
          getDocValues(localId, leafReader, leafOrd, true, funcMap.get(DocValuesType.NUMERIC));
    }

    /**
     * Returns docValues for the specified doc id in the specified reader, if the specified doc
     * holds docValues for this {@link FieldDocValuesSupplier} instance, otherwise returns null.
     *
     * <p>If a non-null value is returned, it will already positioned at the specified docId.
     *
     * @param localId leaf-scoped docId
     * @param leafReader reader containing docId
     * @param leafOrd top-level ord of the specified reader
     */
    public BinaryDocValues getBinaryDocValues(int localId, LeafReader leafReader, int leafOrd)
        throws IOException {
      return (BinaryDocValues)
          getDocValues(localId, leafReader, leafOrd, true, funcMap.get(DocValuesType.BINARY));
    }

    /**
     * Returns docValues for the specified doc id in the specified reader, if the specified doc
     * holds docValues for this {@link FieldDocValuesSupplier} instance, otherwise returns null.
     *
     * <p>If a non-null value is returned, it will already positioned at the specified docId.
     *
     * @param localId leaf-scoped docId
     * @param leafReader reader containing docId
     * @param leafOrd top-level ord of the specified reader
     */
    public SortedDocValues getSortedDocValues(int localId, LeafReader leafReader, int leafOrd)
        throws IOException {
      return (SortedDocValues)
          getDocValues(localId, leafReader, leafOrd, true, funcMap.get(DocValuesType.SORTED));
    }

    /**
     * Returns docValues for the specified doc id in the specified reader, if the specified doc
     * holds docValues for this {@link FieldDocValuesSupplier} instance, otherwise returns null.
     *
     * <p>If a non-null value is returned, it will already positioned at the specified docId, and
     * with values ({@link SortedNumericDocValues#nextValue()}) not yet consumed.
     *
     * @param localId leaf-scoped docId
     * @param leafReader reader containing docId
     * @param leafOrd top-level ord of the specified reader
     */
    public SortedNumericDocValues getSortedNumericDocValues(
        int localId, LeafReader leafReader, int leafOrd) throws IOException {
      return (SortedNumericDocValues)
          getDocValues(
              localId, leafReader, leafOrd, false, funcMap.get(DocValuesType.SORTED_NUMERIC));
    }

    /**
     * Returns docValues for the specified doc id in the specified reader, if the specified doc
     * holds docValues for this {@link FieldDocValuesSupplier} instance, otherwise returns null.
     *
     * <p>If a non-null value is returned, it will already positioned at the specified docId, and
     * with ords ({@link SortedSetDocValues#nextOrd()}) not yet consumed.
     *
     * @param localId leaf-scoped docId
     * @param leafReader reader containing docId
     * @param leafOrd top-level ord of the specified reader
     */
    public SortedSetDocValues getSortedSetDocValues(int localId, LeafReader leafReader, int leafOrd)
        throws IOException {
      return (SortedSetDocValues)
          getDocValues(localId, leafReader, leafOrd, false, funcMap.get(DocValuesType.SORTED_SET));
    }
  }
}
