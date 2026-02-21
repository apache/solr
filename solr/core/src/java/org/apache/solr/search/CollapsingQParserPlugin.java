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

import static org.apache.solr.common.params.CommonParams.SORT;

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.util.IntFloatDynamicMap;
import org.apache.solr.util.IntIntDynamicMap;
import org.apache.solr.util.IntLongDynamicMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>CollapsingQParserPlugin</b> is a PostFilter that performs field collapsing. This is a high
 * performance alternative to standard Solr field collapsing (with ngroups) when the number of
 * distinct groups in the result set is high.
 *
 * <p>Sample syntax:
 *
 * <p>Collapse based on the highest scoring document:
 *
 * <p>fq=(!collapse field=field_name}
 *
 * <p>Collapse based on the min value of a numeric field:
 *
 * <p>fq={!collapse field=field_name min=field_name}
 *
 * <p>Collapse based on the max value of a numeric field:
 *
 * <p>fq={!collapse field=field_name max=field_name}
 *
 * <p>Collapse with a null policy:
 *
 * <p>fq={!collapse field=field_name nullPolicy=nullPolicy}
 *
 * <p>There are three null policies: <br>
 * ignore : removes docs with a null value in the collapse field (default).<br>
 * expand : treats each doc with a null value in the collapse field as a separate group.<br>
 * collapse : collapses all docs with a null value into a single group using either highest score,
 * or min/max.
 *
 * <p>The CollapsingQParserPlugin fully supports the QueryElevationComponent
 */
public class CollapsingQParserPlugin extends QParserPlugin {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NAME = "collapse";
  public static final String HINT_TOP_FC = "top_fc";

  /**
   * Indicates that values in the collapse field are unique per contiguous block, and a single pass
   * "block based" collapse algorithm can be used. This behavior is the default for collapsing on
   * the <code>_root_</code> field, but may also be enabled for other fields that have the same
   * characteristics. This hint will be ignored if other options prevent the use of this single pass
   * approach (notable: nullPolicy=collapse)
   *
   * <p><em>Do <strong>NOT</strong> use this hint if the index is not laid out such that each unique
   * value in the collapse field is garuntteed to only exist in one contiguous block, otherwise the
   * results of the collapse filter will include more then one document per collapse value.</em>
   */
  public static final String HINT_BLOCK = "block";

  /**
   * If elevation is used in combination with the collapse query parser, we can define that we only
   * want to return the representative and not all elevated docs by setting this parameter to false
   * (true by default).
   */
  public static String COLLECT_ELEVATED_DOCS_WHEN_COLLAPSING = "collectElevatedDocsWhenCollapsing";

  public enum NullPolicy {
    IGNORE("ignore"),
    COLLAPSE("collapse"),
    EXPAND("expand");

    private final String name;

    NullPolicy(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public static NullPolicy fromString(String nullPolicy) {
      if (StrUtils.isNullOrEmpty(nullPolicy)) {
        return DEFAULT_POLICY;
      }
      return switch (nullPolicy) {
        case "ignore" -> IGNORE;
        case "collapse" -> COLLAPSE;
        case "expand" -> EXPAND;
        default -> throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Invalid nullPolicy: " + nullPolicy);
      };
    }

    static final NullPolicy DEFAULT_POLICY = IGNORE;
  }

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new CollapsingQParser(qstr, localParams, params, request);
  }

  private static class CollapsingQParser extends QParser {

    public CollapsingQParser(
        String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(qstr, localParams, params, request);
    }

    @Override
    public Query parse() throws SyntaxError {
      try {
        return new CollapsingPostFilter(localParams, params, req);
      } catch (Exception e) {
        throw new SyntaxError(e.getMessage(), e);
      }
    }
  }

  public enum GroupHeadSelectorType {
    MIN,
    MAX,
    SORT,
    SCORE,
    /** For use outside the Solr codebase */
    CUSTOM;
    public static final EnumSet<GroupHeadSelectorType> MIN_MAX = EnumSet.of(MIN, MAX);
  }

  /**
   * Models all the information about how group head documents should be selected
   *
   * @param type The type for this selector, will never be null
   * @param selectorText The param value for this selector whose meaning depends on type. (ie: a
   *     field or valuesource for MIN/MAX, a sort string for SORT, "score" for SCORE). Will never be
   *     null.
   */
  public record GroupHeadSelector(GroupHeadSelectorType type, String selectorText) {

    public GroupHeadSelector {
      assert null != selectorText;
      assert null != type;
    }

    /** returns a new GroupHeadSelector based on the specified local params */
    public static GroupHeadSelector build(final SolrParams localParams) {
      // note: subclasses using CUSTOM should do their own build logic
      final String sortString =
          StrUtils.isBlank(localParams.get(SORT)) ? null : localParams.get(SORT);
      final String max = StrUtils.isBlank(localParams.get("max")) ? null : localParams.get("max");
      final String min = StrUtils.isBlank(localParams.get("min")) ? null : localParams.get("min");

      if (1 < numNotNull(min, max, sortString)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "At most one localparam for selecting documents (min, max, sort) may be specified: "
                + localParams.toString());
      }

      if (null != sortString) {
        return new GroupHeadSelector(GroupHeadSelectorType.SORT, sortString);
      } else if (null != min) {
        return new GroupHeadSelector(GroupHeadSelectorType.MIN, min);
      } else if (null != max) {
        return new GroupHeadSelector(GroupHeadSelectorType.MAX, max);
      }
      // default
      return new GroupHeadSelector(GroupHeadSelectorType.SCORE, "score");
    }

    /**
     * Determines if scores are needed for collapsing based on this selector's type and
     * configuration.
     *
     * @param sortSpec the SortSpec if this selector is SORT type, otherwise may be null
     * @return true if scores are needed for the collapsing operation
     */
    public boolean needsScores(SortSpec sortSpec) {
      return GroupHeadSelectorType.SCORE.equals(this.type)
          || (GroupHeadSelectorType.SORT.equals(this.type) && sortSpec.includesScore())
          || (GroupHeadSelectorType.MIN_MAX.contains(this.type)
              && CollapseScore.wantsCScore(this.selectorText));
    }
  }

  public static class CollapsingPostFilter extends ExtendedQueryBase implements PostFilter {

    protected final String collapseField;
    protected final GroupHeadSelector groupHeadSelector;
    protected final SortSpec sortSpec; // may be null, parsed at most once from groupHeadSelector
    public final String hint;
    protected final boolean needsScores4Collapsing;
    protected final NullPolicy nullPolicy;
    protected final boolean collectElevatedDocsWhenCollapsing;
    protected final int initialSize;

    public String getField() {
      return this.collapseField;
    }

    @Override
    public void setCache(boolean cache) {}

    @Override
    public boolean getCache() {
      return false;
    }

    // Only a subset of fields in hashCode/equals?

    @Override
    public int hashCode() {
      int hashCode = classHash();
      hashCode = 31 * hashCode + collapseField.hashCode();
      hashCode = 31 * hashCode + groupHeadSelector.hashCode();
      hashCode = 31 * hashCode + nullPolicy.hashCode();
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(CollapsingPostFilter other) {
      return collapseField.equals(other.collapseField)
          && groupHeadSelector.equals(other.groupHeadSelector)
          && nullPolicy == other.nullPolicy
          && collectElevatedDocsWhenCollapsing == other.collectElevatedDocsWhenCollapsing;
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public int getCost() {
      return Math.max(super.getCost(), 100);
    }

    @Override
    public String toString(String s) {
      return "CollapsingPostFilter(field="
          + this.collapseField
          + ", nullPolicy="
          + this.nullPolicy.getName()
          + ", "
          + this.groupHeadSelector
          + (hint == null ? "" : ", hint=" + this.hint)
          + ", size="
          + this.initialSize
          + ")";
    }

    public CollapsingPostFilter(
        SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      // Don't allow collapsing if grouping is being used.
      if (request.getParams().getBool(GroupParams.GROUP, false)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Can not use collapse with Grouping enabled");
      }

      this.collapseField = localParams.get("field");
      if (this.collapseField == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Required 'field' param is missing.");
      }

      // if unknown field, this would fail fast
      SchemaField collapseFieldSf = request.getSchema().getField(this.collapseField);
      if (!(collapseFieldSf.isUninvertible() || collapseFieldSf.hasDocValues())) {
        // uninvertible=false and docvalues=false
        // field can't be indexed=false and uninvertible=true
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Collapsing field '"
                + collapseField
                + "' should be either docValues enabled or indexed with uninvertible enabled");
      } else if (collapseFieldSf.multiValued()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Collapsing not supported on multivalued fields");
      }

      this.groupHeadSelector = buildGroupHeadSelector(localParams);

      if (groupHeadSelector.type.equals(GroupHeadSelectorType.SORT)
          && CollapseScore.wantsCScore(groupHeadSelector.selectorText)) {
        // we can't support Sorts that wrap functions that include "cscore()" because
        // the abstraction layer for Sort/SortField rewriting gives each clause it's own
        // context Map which we don't have access to -- so for now, give a useful error
        // (as early as possible) if attempted
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Using cscore() as a function in the 'sort' local "
                + "param of the collapse parser is not supported");
      }

      this.sortSpec =
          GroupHeadSelectorType.SORT.equals(groupHeadSelector.type)
              ? SortSpecParsing.parseSortSpec(groupHeadSelector.selectorText, request)
              : null;

      this.hint = localParams.get("hint");
      this.initialSize =
          localParams.getInt("size", 100_000); // Only used for collapsing on int fields.

      {
        final SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        assert null != info;

        // may be null in some esoteric corner usages
        final ResponseBuilder rb = info.getResponseBuilder();

        this.needsScores4Collapsing = groupHeadSelector.needsScores(this.sortSpec);

        if (this.needsScores4Collapsing && null != rb) {
          // ensure the IndexSearcher will compute scores for the "real" docs
          // when we need them to find the groupHead
          rb.setFieldFlags(rb.getFieldFlags() | SolrIndexSearcher.GET_SCORES);
        }
      }

      this.nullPolicy = NullPolicy.fromString(localParams.get("nullPolicy"));

      this.collectElevatedDocsWhenCollapsing =
          params.getBool(COLLECT_ELEVATED_DOCS_WHEN_COLLAPSING, true);
    }

    protected GroupHeadSelector buildGroupHeadSelector(SolrParams localParams) {
      return GroupHeadSelector.build(localParams);
    }

    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      try {
        return this.getFilterCollector((SolrIndexSearcher) indexSearcher);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected DelegatingCollector getFilterCollector(SolrIndexSearcher searcher)
        throws IOException {
      // We have to deal with it here rather than the constructor because
      //  the QueryElevationComponent runs after the Queries are constructed.
      IntIntHashMap boostDocs = getElevatedBoostDocsMap(searcher);

      // block collapsing logic is much simpler and uses less memory, but is only viable in specific
      // situations
      final boolean blockCollapse =
          (("_root_".equals(collapseField) || HINT_BLOCK.equals(hint))
              // because we currently handle all min/max cases using
              // AbstractBlockSortSpecCollector, we can't handle functions wrapping cscore()
              // (for the same reason cscore() isn't supported in 'sort' local param)
              && (!CollapseScore.wantsCScore(groupHeadSelector.selectorText))
              //
              && NullPolicy.COLLAPSE != nullPolicy);
      if (HINT_BLOCK.equals(hint) && !blockCollapse) {
        log.debug(
            "Query specifies hint={} but other local params prevent the use block based collapse",
            HINT_BLOCK);
      }

      SchemaField schemaField = searcher.getSchema().getField(collapseField);
      DocValuesProducer docValuesProducer =
          getDocValuesProducer(schemaField, hint, searcher, blockCollapse);
      FieldType collapseFieldType = schemaField.getType();

      FunctionQuery funcQuery = null;
      FieldType minMaxFieldType = null;
      if (GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type)) {
        final String text = groupHeadSelector.selectorText;
        if (!text.contains("(")) {
          minMaxFieldType = searcher.getSchema().getField(text).getType();
        } else {
          SolrParams params = new ModifiableSolrParams();
          try (SolrQueryRequest request = SolrQueryRequest.wrapSearcher(searcher, params)) {
            FunctionQParser functionQParser = new FunctionQParser(text, null, params, request);
            funcQuery = (FunctionQuery) functionQParser.parse();
          } catch (SyntaxError e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
        }
      }

      if (GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type)) {

        if (collapseFieldType instanceof StrField) {
          if (blockCollapse) {
            return new BlockOrdScoreCollector(collapseField, nullPolicy, boostDocs);
          }
          return new OrdScoreCollector(searcher, docValuesProducer, nullPolicy, boostDocs);

        } else if (isNumericCollapsible(collapseFieldType)) {
          if (blockCollapse) {
            return new BlockIntScoreCollector(collapseField, nullPolicy, boostDocs);
          }

          return new IntScoreCollector(searcher, nullPolicy, boostDocs, collapseField, initialSize);

        } else {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Collapsing field should be of either String, Int or Float type");
        }

      } else { // min, max, sort, etc.. something other then just "score"

        if (collapseFieldType instanceof StrField) {
          if (blockCollapse) {
            // NOTE: for now we don't worry about whether this is a sortSpec of min/max
            // groupHeadSelector, we use a "sort spec' based block collector unless/until there is
            // some (performance?) reason to specialize
            return new BlockOrdSortSpecCollector(
                collapseField,
                nullPolicy,
                boostDocs,
                BlockOrdSortSpecCollector.getSort(
                    groupHeadSelector, sortSpec, funcQuery, searcher));
          }

          return new OrdFieldCollectorBuilder(
                  groupHeadSelector,
                  docValuesProducer,
                  nullPolicy,
                  needsScores4Collapsing,
                  boostDocs,
                  searcher)
              .build(sortSpec, minMaxFieldType, funcQuery);

        } else if (isNumericCollapsible(collapseFieldType)) {

          if (blockCollapse) {
            // NOTE: for now we don't worry about whether this is a sortSpec of min/max
            // groupHeadSelector, we use a "sort spec' based block collector unless/until there is
            // some (performance?) reason to specialize
            return new BlockIntSortSpecCollector(
                collapseField,
                nullPolicy,
                boostDocs,
                BlockOrdSortSpecCollector.getSort(
                    groupHeadSelector, sortSpec, funcQuery, searcher));
          }

          return new IntFieldCollectorBuilder(
                  groupHeadSelector,
                  nullPolicy,
                  collapseField,
                  initialSize,
                  needsScores4Collapsing,
                  boostDocs,
                  searcher)
              .build(sortSpec, minMaxFieldType, funcQuery);
        } else {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Collapsing field should be of either String, Int or Float type");
        }
      }
    }

    @SuppressWarnings({"unchecked"})
    protected IntIntHashMap getElevatedBoostDocsMap(SolrIndexSearcher indexSearcher)
        throws IOException {
      if (!collectElevatedDocsWhenCollapsing) {
        return null;
      }
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        var context = info.getReq().getContext();
        var boosted = (Set<BytesRef>) context.get(QueryElevationComponent.BOOSTED);
        return QueryElevationComponent.getBoostDocs(indexSearcher, boosted, context);
      }
      return null;
    }

    private boolean isNumericCollapsible(FieldType collapseFieldType) {
      return switch (collapseFieldType.getNumberType()) {
        case NumberType.INTEGER, NumberType.FLOAT -> true;
        default -> false;
      };
    }

    protected DocValuesProducer getDocValuesProducer(
        SchemaField schemaField, String hint, SolrIndexSearcher searcher, boolean blockCollapse) {
      if (schemaField.getType() instanceof StrField) {
        // if we are using blockCollapse, then there is no need to bother with TOP_FC
        if (HINT_TOP_FC.equals(hint) && !blockCollapse) {
          @SuppressWarnings("resource")
          final LeafReader uninvertingReader = getTopFieldCacheReader(searcher, collapseField);

          return new EmptyDocValuesProducer() {
            @Override
            public SortedDocValues getSorted(FieldInfo ignored) throws IOException {
              SortedDocValues values = uninvertingReader.getSortedDocValues(collapseField);
              if (values != null) {
                return values;
              } else {
                return DocValues.emptySorted();
              }
            }
          };
        } else {
          return new EmptyDocValuesProducer() {
            @Override
            public SortedDocValues getSorted(FieldInfo ignored) throws IOException {
              return DocValues.getSorted(searcher.getSlowAtomicReader(), collapseField);
            }
          };
        }
      } else {
        if (HINT_TOP_FC.equals(hint)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "top_fc hint is only supported when collapsing on String Fields");
        }
      }
      return null;
    }
  }

  /**
   * This forces the use of the top level field cache for String fields. This is VERY fast at query
   * time but slower to warm and causes insanity.
   */
  public static LeafReader getTopFieldCacheReader(
      SolrIndexSearcher searcher, String collapseField) {
    UninvertingReader.Type type = null;
    final SchemaField f = searcher.getSchema().getFieldOrNull(collapseField);
    assert null != f; // should already be enforced higher up
    assert !f.multiValued(); // should already be enforced higher up

    assert f.getType() instanceof StrField; // this method shouldn't be called otherwise
    if (f.indexed() && f.isUninvertible()) {
      type = UninvertingReader.Type.SORTED;
    }

    return UninvertingReader.wrap(
        new ReaderWrapper(searcher.getSlowAtomicReader(), collapseField),
        Collections.singletonMap(collapseField, type)::get);
  }

  private static class ReaderWrapper extends FilterLeafReader {

    private final FieldInfos fieldInfos;

    ReaderWrapper(LeafReader leafReader, String field) {
      super(leafReader);

      // TODO can we just do "field" and not bother with the other fields?
      List<FieldInfo> newInfos = new ArrayList<>(in.getFieldInfos().size());
      for (FieldInfo fieldInfo : in.getFieldInfos()) {
        if (fieldInfo.name.equals(field)) {
          FieldInfo f =
              new FieldInfo(
                  fieldInfo.name,
                  fieldInfo.number,
                  fieldInfo.hasTermVectors(),
                  fieldInfo.hasNorms(),
                  fieldInfo.hasPayloads(),
                  fieldInfo.getIndexOptions(),
                  DocValuesType.NONE,
                  DocValuesSkipIndexType.NONE,
                  fieldInfo.getDocValuesGen(),
                  fieldInfo.attributes(),
                  fieldInfo.getPointDimensionCount(),
                  fieldInfo.getPointIndexDimensionCount(),
                  fieldInfo.getPointNumBytes(),
                  fieldInfo.getVectorDimension(),
                  fieldInfo.getVectorEncoding(),
                  fieldInfo.getVectorSimilarityFunction(),
                  fieldInfo.isSoftDeletesField(),
                  fieldInfo.isParentField());
          newInfos.add(f);
        } else {
          newInfos.add(fieldInfo);
        }
      }
      FieldInfos infos = new FieldInfos(newInfos.toArray(new FieldInfo[0]));
      this.fieldInfos = infos;
    }

    @Override
    public FieldInfos getFieldInfos() {
      return fieldInfos;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
      return null;
    }

    // NOTE: delegating the caches is wrong here as we are altering the content
    // of the reader, this should ONLY be used under an uninvertingreader which
    // will restore doc values back using uninversion, otherwise all sorts of
    // crazy things could happen.

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  private static class ScoreAndDoc extends Scorable {

    public float score;
    public int docId;

    @Override
    public float score() {
      return score;
    }
  }

  private abstract static class CachedScoreScorable extends Scorable {
    private final DocIdSetIterator disi;
    private int cachedDocId = -1;
    private float cachedScore;

    CachedScoreScorable(DocIdSetIterator disi) {
      this.disi = disi;
    }

    @Override
    public final float score() throws IOException {
      int docId = disi.docID();
      if (docId != cachedDocId) {
        cachedDocId = docId;
        cachedScore = computeScore(docId);
      }
      return cachedScore;
    }

    protected abstract float computeScore(int globalDoc) throws IOException;
  }

  /**
   * Abstract base class for all collapse collectors. Provides common configuration and result state
   * shared across ordinal and integer-based collapse strategies.
   *
   * @lucene.internal
   */
  protected abstract static class AbstractCollapseCollector extends DelegatingCollector {
    // Configuration
    protected final List<LeafReaderContext> contexts;
    protected final int maxDoc;
    protected final NullPolicy nullPolicy;
    protected final boolean needsScores4Collapsing;
    protected boolean needsScores; // cached from scoreMode()

    // Results/accumulator
    protected final DocIdSetBuilder collapsedSet;
    protected final BoostedDocsCollector boostedDocsCollector;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected int nullDoc = -1;

    protected AbstractCollapseCollector(
        IndexSearcher searcher,
        NullPolicy nullPolicy,
        boolean needsScores4Collapsing,
        IntIntHashMap boostDocsMap) {

      this.contexts = searcher.getTopReaderContext().leaves();
      this.maxDoc = searcher.getIndexReader().maxDoc();
      this.nullPolicy = nullPolicy;
      this.needsScores4Collapsing = needsScores4Collapsing;

      this.collapsedSet = new DocIdSetBuilder(Math.max(1, maxDoc));
      this.boostedDocsCollector = BoostedDocsCollector.build(boostDocsMap);
    }

    @Override
    public ScoreMode scoreMode() {
      return needsScores4Collapsing ? ScoreMode.COMPLETE : super.scoreMode();
    }

    /**
     * Initialize data structures for collection. Called once before collecting begins. This is
     * useful for initialization dependent on {@link #needsScores}, which isn't known in the
     * constructor.
     */
    protected void initializeCollection(boolean needsScores) {
      if (needsScores && nullPolicy == NullPolicy.EXPAND) {
        this.nullScores = new FloatArrayList();
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      if (this.context == null) { // first time detection
        initializeCollection(this.needsScores = scoreMode().needsScores());
      }

      // Do NOT set leafDelegate (calling super() would do this).
      // That's handled in complete() for these collectors.
      assert this.contexts.get(context.ord) == context;
      assert leafDelegate == null;
      this.context = context;
      this.docBase = context.docBase;
    }

    /** Core/general algorithm */
    @Override
    public final void complete() throws IOException {

      DocIdSetIterator collapsedDocs = getCollapsedDocs();

      int nextDocBase = 0;
      int globalDoc;

      while ((globalDoc = collapsedDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (globalDoc >= nextDocBase) {
          // finish previous leaf
          if (leafDelegate != null) {
            leafDelegate.finish();
          }

          int ctxIdx = ReaderUtil.subIndex(globalDoc, contexts);
          context = contexts.get(ctxIdx);
          docBase = context.docBase;
          nextDocBase = ctxIdx + 1 < contexts.size() ? contexts.get(ctxIdx + 1).docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(context);
          if (delegate.scoreMode().needsScores()) {
            scorer = getCollapsedScores(collapsedDocs, context);
            leafDelegate.setScorer(scorer);
          }
        }

        leafDelegate.collect(globalDoc - docBase);
      }

      if (leafDelegate != null) {
        leafDelegate.finish();
      }

      super.complete();
    }

    /**
     * Produce the final list of global docs that we'll pass on through to delegated/chained
     * collectors. This is the first step of {@link #complete()}.
     */
    protected DocIdSetIterator getCollapsedDocs() throws IOException {
      // all subclasses have this common logic to do first regarding boosted/elevated & nullDoc:

      boostedDocsCollector.addBoostedDocsTo(collapsedSet);

      if (boostedDocsCollector.isBoostedNullGroup()) {
        // If we're using IGNORE then no (matching) null docs were collected (by caller)
        // If we're using EXPAND then all (matching) null docs were already collected (by us)
        //   ...and that's *good* because each is treated like its own group, our boosts don't
        // matter
        // We only have to worry about removing null docs when using COLLAPSE, in which case any
        // boosted null doc means we clear the group head of the null group.
        nullDoc = -1;
      }

      if (nullDoc > -1) {
        collapsedSet.grow(1).add(nullDoc);
      }

      finishCollapsedSet();

      DocIdSetIterator iterator = collapsedSet.build().iterator();
      return iterator != null ? iterator : DocIdSetIterator.empty();
    }

    /**
     * Finishes adding docs to {@link #collapsedSet} so that it's ready. First step is usually to
     * call {@link BoostedDocsCollector#visitBoostedGroupKeys(IntProcedure)} to ensure we don't add
     * docs for these group keys as they have already been added by the caller, which are query
     * elevated / boosted docs. Called by {@link #getCollapsedDocs()}
     */
    protected abstract void finishCollapsedSet();

    /**
     * Return a {@link Scorable} that provides the score for the current doc in the given {@code
     * disi} (global doc IDs). Called once per segment during {@link #complete()}.
     */
    protected abstract Scorable getCollapsedScores(DocIdSetIterator disi, LeafReaderContext context)
        throws IOException;

    private MergeBoost mergeBoost;
    private int nullScoreIndex = 0; // next nullScores index to return from scoreNullGroup if EXPAND

    /** Returns the score for a null-group doc during {@link #complete()}. Only called once. */
    protected float scoreNullGroup(int globalDoc) {
      if (mergeBoost == null) { // lazy init
        mergeBoost = boostedDocsCollector.getMergeBoost();
      }
      if (mergeBoost.boost(globalDoc)) { // has side effect
        return 0F;
      } else if (nullPolicy == NullPolicy.COLLAPSE) {
        return nullScore;
      } else if (nullPolicy == NullPolicy.EXPAND) {
        return nullScores.get(nullScoreIndex++); // side effect
      }
      return 0F;
    }
  }

  /**
   * Collapses on Ordinal Values using Score to select the group head.
   *
   * @lucene.internal
   */
  static class OrdScoreCollector extends AbstractCollapseCollector {

    // Source data
    private final DocValuesProducer collapseValuesProducer;
    private SortedDocValues collapseValues;
    private OrdinalMap ordinalMap;
    private MultiDocValues.MultiSortedDocValues multiSortedDocValues;
    private SortedDocValues segmentValues;
    private LongValues segmentOrdinalMap;

    // Results/accumulator
    private final IntIntDynamicMap ords;
    private final IntFloatDynamicMap scores;

    public OrdScoreCollector(
        IndexSearcher searcher,
        DocValuesProducer collapseValuesProducer,
        NullPolicy nullPolicy,
        IntIntHashMap boostDocsMap)
        throws IOException {
      super(searcher, nullPolicy, true, boostDocsMap);

      this.collapseValuesProducer = collapseValuesProducer;
      initCollapseValues();
      int valueCount = collapseValues.getValueCount();

      this.ords = new IntIntDynamicMap(valueCount, -1);
      this.scores = new IntFloatDynamicMap(valueCount, -Float.MAX_VALUE);
      this.nullScore = -Float.MAX_VALUE;
    }

    private void initCollapseValues() throws IOException {
      this.collapseValues = collapseValuesProducer.getSorted(null);
      if (collapseValues instanceof MultiDocValues.MultiSortedDocValues multi) {
        this.multiSortedDocValues = multi;
        this.ordinalMap = multi.mapping;
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      initSegmentValues(context);
    }

    private void initSegmentValues(LeafReaderContext context) {
      if (ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[context.ord];
        this.segmentOrdinalMap = ordinalMap.getGlobalOrds(context.ord);
      } else {
        this.segmentValues = collapseValues;
      }
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      int globalDoc = contextDoc + this.docBase;
      int ord = -1;
      if (this.ordinalMap != null) {
        // Handle ordinalMapping case
        if (segmentValues.advanceExact(contextDoc)) {
          ord = (int) segmentOrdinalMap.get(segmentValues.ordValue());
        } else {
          ord = -1;
        }
      } else {
        // Handle top Level FieldCache or Single Segment Case
        if (segmentValues.advanceExact(globalDoc)) {
          ord = segmentValues.ordValue();
        } else {
          ord = -1;
        }
      }

      if (boostedDocsCollector.hasBoosts()) {
        if (0 <= ord) {
          if (boostedDocsCollector.collectIfBoosted(ord, globalDoc)) return;
        } else {
          if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        }
      }

      if (ord > -1) {
        float score = scorer.score();
        if (score > scores.get(ord)) {
          ords.put(ord, globalDoc);
          scores.put(ord, score);
        }
      } else if (nullPolicy == NullPolicy.COLLAPSE) {
        float score = scorer.score();
        if (score > nullScore) {
          nullScore = score;
          nullDoc = globalDoc;
        }
      } else if (nullPolicy == NullPolicy.EXPAND) {
        collapsedSet.grow(1).add(globalDoc);
        nullScores.add(scorer.score());
      }
    }

    @Override
    protected void finishCollapsedSet() {
      boostedDocsCollector.visitBoostedGroupKeys(ords::remove);

      DocIdSetBuilder.BulkAdder adder = collapsedSet.grow(ords.size());
      ords.forEachValue(adder::add);
    }

    private boolean collapsedScoresInitialized;

    @Override
    protected Scorable getCollapsedScores(DocIdSetIterator disi, LeafReaderContext context)
        throws IOException {
      if (!collapsedScoresInitialized) {
        collapsedScoresInitialized = true;
        initCollapseValues();
      }
      initSegmentValues(context);
      return new CachedScoreScorable(disi) {
        @Override
        protected float computeScore(int globalDoc) throws IOException {
          int ord = -1;
          if (ordinalMap != null) {
            int contextDoc = globalDoc - context.docBase;
            if (segmentValues.advanceExact(contextDoc)) {
              ord = (int) segmentOrdinalMap.get(segmentValues.ordValue());
            }
          } else {
            if (segmentValues.advanceExact(globalDoc)) {
              ord = segmentValues.ordValue();
            }
          }
          if (ord > -1) {
            return scores.get(ord);
          }
          return scoreNullGroup(globalDoc);
        }
      };
    }
  }

  /**
   * Collapses on an integer field using the score to select the group head.
   *
   * @lucene.internal
   */
  static class IntScoreCollector extends AbstractCollapseCollector {

    // Configuration
    private final String field;

    // Source data
    private NumericDocValues collapseValues;

    // Results/accumulator
    private final IntLongHashMap cmap;

    public IntScoreCollector(
        SolrIndexSearcher searcher,
        NullPolicy nullPolicy,
        IntIntHashMap boostDocsMap,
        String field,
        int initialSize) {
      super(searcher, nullPolicy, true, boostDocsMap);

      this.field = field;

      this.cmap = new IntLongHashMap(initialSize);
      this.nullScore = -Float.MAX_VALUE;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.collapseValues = DocValues.getNumeric(context.reader(), this.field);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      final int globalDoc = docBase + contextDoc;
      if (collapseValues.advanceExact(contextDoc)) {
        final int collapseValue = (int) collapseValues.longValue();

        if (boostedDocsCollector.collectIfBoosted(collapseValue, globalDoc)) return;

        float score = scorer.score();
        final int idx;
        if ((idx = cmap.indexOf(collapseValue)) >= 0) {
          long scoreDoc = cmap.indexGet(idx);
          int testScore = (int) (scoreDoc >> 32);
          int currentScore = Float.floatToRawIntBits(score);
          if (currentScore > testScore) {
            // Current score is higher so replace the old scoreDoc with the current scoreDoc
            cmap.indexReplace(idx, (((long) currentScore) << 32) + globalDoc);
          }
        } else {
          // Combine the score and document into a long.
          long scoreDoc = (((long) Float.floatToRawIntBits(score)) << 32) + globalDoc;
          cmap.indexInsert(idx, collapseValue, scoreDoc);
        }

      } else { // Null Group...

        if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;

        if (nullPolicy == NullPolicy.COLLAPSE) {
          float score = scorer.score();
          if (score > this.nullScore) {
            this.nullScore = score;
            this.nullDoc = globalDoc;
          }
        } else if (nullPolicy == NullPolicy.EXPAND) {
          collapsedSet.grow(1).add(globalDoc);
          nullScores.add(scorer.score());
        }
      }
    }

    @Override
    protected void finishCollapsedSet() {
      boostedDocsCollector.visitBoostedGroupKeys(cmap::remove);

      DocIdSetBuilder.BulkAdder adder = collapsedSet.grow(cmap.size());
      for (IntLongCursor cursor : cmap) {
        // the low bits of the long is the global doc ID
        adder.add((int) cursor.value);
      }
    }

    @Override
    protected Scorable getCollapsedScores(DocIdSetIterator disi, LeafReaderContext context)
        throws IOException {
      this.collapseValues = DocValues.getNumeric(context.reader(), this.field);
      return new CachedScoreScorable(disi) {
        @Override
        protected float computeScore(int globalDoc) throws IOException {
          int contextDoc = globalDoc - context.docBase;
          if (collapseValues.advanceExact(contextDoc)) {
            int collapseValue = (int) collapseValues.longValue();
            long scoreDoc = cmap.get(collapseValue);
            return Float.intBitsToFloat((int) (scoreDoc >> 32));
          }
          return scoreNullGroup(globalDoc);
        }
      };
    }
  }

  /** Builder for {@link OrdFieldValueCollector}. */
  public record OrdFieldCollectorBuilder(
      GroupHeadSelector groupHeadSelector,
      DocValuesProducer collapseValuesProducer,
      NullPolicy nullPolicy,
      boolean needsScores4Collapsing,
      IntIntHashMap boostDocsMap,
      SolrIndexSearcher searcher) {

    /** Builds the appropriate OrdFieldValueCollector subclass based on the selection strategy. */
    public DelegatingCollector build(
        SortSpec sortSpec, FieldType fieldType, FunctionQuery funcQuery) throws IOException {

      if (null != sortSpec) {
        return new OrdSortSpecCollector(this, sortSpec);
      } else if (funcQuery != null) {
        return new OrdValueSourceCollector(this, funcQuery);
      } else {
        NumberType numType = fieldType.getNumberType();
        if (null == numType) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "min/max must be either Int/Long/Float based field types");
        }
        return switch (numType) {
          case INTEGER -> new OrdIntCollector(this);
          case FLOAT -> new OrdFloatCollector(this);
          case LONG -> new OrdLongCollector(this);
          default -> throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "min/max must be either Int/Long/Float field types");
        };
      }
    }
  }

  /**
   * Collapse on Ordinal value field.
   *
   * @lucene.internal
   */
  protected abstract static class OrdFieldValueCollector extends AbstractCollapseCollector {
    // Source data
    protected final int valueCount;
    protected final DocValuesProducer collapseValuesProducer;
    protected SortedDocValues collapseValues;
    protected OrdinalMap ordinalMap;
    protected MultiDocValues.MultiSortedDocValues multiSortedDocValues;
    protected SortedDocValues segmentValues;
    protected LongValues segmentOrdinalMap;

    // Results/accumulator
    protected final IntIntDynamicMap ords;
    protected IntFloatDynamicMap scores;

    protected OrdFieldValueCollector(OrdFieldCollectorBuilder ctx) throws IOException {
      super(ctx.searcher, ctx.nullPolicy, ctx.needsScores4Collapsing, ctx.boostDocsMap);

      this.collapseValuesProducer = ctx.collapseValuesProducer;

      initCollapseValues();
      this.valueCount = collapseValues.getValueCount();

      this.ords = new IntIntDynamicMap(valueCount, -1);
    }

    private void initCollapseValues() throws IOException {
      this.collapseValues = collapseValuesProducer.getSorted(null);
      if (collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues) collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }
    }

    @Override
    protected void initializeCollection(boolean needsScores) {
      super.initializeCollection(needsScores);
      if (needsScores) {
        this.scores = new IntFloatDynamicMap(valueCount, 0.0f);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      initSegmentValues(context);
    }

    private void initSegmentValues(LeafReaderContext context) {
      if (ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[context.ord];
        this.segmentOrdinalMap = ordinalMap.getGlobalOrds(context.ord);
      } else {
        this.segmentValues = collapseValues;
      }
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      int globalDoc = contextDoc + this.docBase;
      int ord = -1;
      if (this.ordinalMap != null) {
        if (segmentValues.advanceExact(contextDoc)) {
          ord = (int) segmentOrdinalMap.get(segmentValues.ordValue());
        }
      } else {
        if (segmentValues.advanceExact(globalDoc)) {
          ord = segmentValues.ordValue();
        }
      }

      if (boostedDocsCollector.hasBoosts()) {
        if (-1 == ord) {
          if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        } else {
          if (boostedDocsCollector.collectIfBoosted(ord, globalDoc)) return;
        }
      }

      collapse(ord, contextDoc, globalDoc);
    }

    /**
     * Strategy-specific collapse logic. Subclasses implement this to define how to select the group
     * head document.
     */
    protected abstract void collapse(int ord, int contextDoc, int globalDoc) throws IOException;

    @Override
    protected void finishCollapsedSet() {
      boostedDocsCollector.visitBoostedGroupKeys(ords::remove);

      DocIdSetBuilder.BulkAdder adder = collapsedSet.grow(ords.size());
      ords.forEachValue(adder::add);
    }

    private boolean collapsedScoresInitialized;

    @Override
    protected Scorable getCollapsedScores(DocIdSetIterator disi, LeafReaderContext context)
        throws IOException {
      if (!collapsedScoresInitialized) {
        collapsedScoresInitialized = true;
        initCollapseValues();
      }

      initSegmentValues(context);

      return new CachedScoreScorable(disi) {
        @Override
        protected float computeScore(int globalDoc) throws IOException {
          int ord = -1;
          if (ordinalMap != null) {
            int contextDoc = globalDoc - context.docBase;
            if (segmentValues.advanceExact(contextDoc)) {
              ord = (int) segmentOrdinalMap.get(segmentValues.ordValue());
            }
          } else {
            if (segmentValues.advanceExact(globalDoc)) {
              ord = segmentValues.ordValue();
            }
          }
          if (ord > -1) {
            return scores.get(ord);
          }
          return scoreNullGroup(globalDoc);
        }
      };
    }
  }

  /** Collector for collapsing on ordinal using min/max of an int field to select group head. */
  protected static class OrdIntCollector extends OrdFieldValueCollector {
    // Configuration
    private final String field;
    private final IntCompare comp;

    // Source data
    private NumericDocValues minMaxValues;

    // Results/accumulator
    private final IntIntDynamicMap ordVals;
    private int nullVal;

    public OrdIntCollector(OrdFieldCollectorBuilder ctx) throws IOException {
      super(ctx);
      this.field = ctx.groupHeadSelector.selectorText;
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxIntComp();
        this.ordVals = new IntIntDynamicMap(valueCount, Integer.MIN_VALUE);
      } else {
        comp = new MinIntComp();
        this.ordVals = new IntIntDynamicMap(valueCount, Integer.MAX_VALUE);
        this.nullVal = Integer.MAX_VALUE;
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.minMaxValues = DocValues.getNumeric(context.reader(), this.field);
    }

    @Override
    protected void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      int currentVal;
      if (minMaxValues.advanceExact(contextDoc)) {
        currentVal = (int) minMaxValues.longValue();
      } else {
        currentVal = 0;
      }

      if (ord > -1) {
        if (comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if (needsScores) {
            scores.put(ord, scorer.score());
          }
        }
      } else if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  /** Collector for collapsing on ordinal using min/max of a float field to select group head. */
  protected static class OrdFloatCollector extends OrdFieldValueCollector {
    // Configuration
    private final String field;
    private final FloatCompare comp;

    // Source data
    private NumericDocValues minMaxValues;

    // Results/accumulator
    private final IntFloatDynamicMap ordVals;
    private float nullVal;

    public OrdFloatCollector(OrdFieldCollectorBuilder ctx) throws IOException {
      super(ctx);
      this.field = ctx.groupHeadSelector.selectorText;
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, -Float.MAX_VALUE);
        this.nullVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, Float.MAX_VALUE);
        this.nullVal = Float.MAX_VALUE;
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.minMaxValues = DocValues.getNumeric(context.reader(), this.field);
    }

    @Override
    protected void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      int currentMinMax;
      if (minMaxValues.advanceExact(contextDoc)) {
        currentMinMax = (int) minMaxValues.longValue();
      } else {
        currentMinMax = 0;
      }

      float currentVal = Float.intBitsToFloat(currentMinMax);

      if (ord > -1) {
        if (comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if (needsScores) {
            scores.put(ord, scorer.score());
          }
        }
      } else if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  /** Collector for collapsing on ordinal using min/max of a long field to select group head. */
  protected static class OrdLongCollector extends OrdFieldValueCollector {
    // Configuration
    private final String field;
    private final LongCompare comp;

    // Source data
    private NumericDocValues minMaxVals;

    // Results/accumulator
    private final IntLongDynamicMap ordVals;
    private long nullVal;

    public OrdLongCollector(OrdFieldCollectorBuilder ctx) throws IOException {
      super(ctx);

      this.field = ctx.groupHeadSelector.selectorText;
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxLongComp();
        this.ordVals = new IntLongDynamicMap(valueCount, Long.MIN_VALUE);
      } else {
        this.nullVal = Long.MAX_VALUE;
        comp = new MinLongComp();
        this.ordVals = new IntLongDynamicMap(valueCount, Long.MAX_VALUE);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    @Override
    protected void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      long currentVal;
      if (minMaxVals.advanceExact(contextDoc)) {
        currentVal = minMaxVals.longValue();
      } else {
        currentVal = 0;
      }

      if (ord > -1) {
        if (comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if (needsScores) {
            scores.put(ord, scorer.score());
          }
        }
      } else if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  /**
   * Collector for collapsing on ordinal using min/max of a value source function to select group
   * head.
   */
  protected static class OrdValueSourceCollector extends OrdFieldValueCollector {
    // Configuration
    private final FloatCompare comp;
    private final ValueSource valueSource;
    private final Map<Object, Object> rcontext;
    private final CollapseScore collapseScore = new CollapseScore();

    // Source data
    private FunctionValues functionValues;

    // Results/accumulator
    private final IntFloatDynamicMap ordVals;
    private float nullVal;

    public OrdValueSourceCollector(OrdFieldCollectorBuilder ctx, FunctionQuery funcQuery)
        throws IOException {
      super(ctx);
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, -Float.MAX_VALUE);
      } else {
        this.nullVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, Float.MAX_VALUE);
      }
      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(ctx.searcher);
      collapseScore.setupIfNeeded(ctx.groupHeadSelector, rcontext);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      functionValues = this.valueSource.getValues(rcontext, context);
    }

    @Override
    protected void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      float score = 0;

      if (needsScores4Collapsing) {
        score = scorer.score();
        this.collapseScore.score = score;
      }

      float currentVal = functionValues.floatVal(contextDoc);

      if (ord > -1) {
        if (comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if (needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores.put(ord, score);
          }
        }
      } else if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            this.nullScore = score;
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  /**
   * Collector for collapsing on ordinal using the first document according to a complex sort as the
   * group head.
   */
  protected static class OrdSortSpecCollector extends OrdFieldValueCollector {
    // Configuration
    private final SortFieldsCompare compareState;

    // Results/accumulator
    private float score;

    public OrdSortSpecCollector(OrdFieldCollectorBuilder ctx, SortSpec sortSpec)
        throws IOException {
      super(ctx);

      assert GroupHeadSelectorType.SORT.equals(ctx.groupHeadSelector.type);

      Sort sort = rewriteSort(sortSpec, ctx.searcher);
      this.compareState = new SortFieldsCompare(sort.getSort(), valueCount);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      compareState.setNextReader(context);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      super.setScorer(scorer);
      this.compareState.setScorer(scorer);
    }

    @Override
    protected void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      if (needsScores4Collapsing) {
        this.score = scorer.score();
      }

      if (ord > -1) { // real collapseKey
        if (-1 == ords.get(ord)) {
          // we've never seen this ord (aka: collapseKey) before, treat it as group head for now
          compareState.setGroupValues(ord, contextDoc);
          ords.put(ord, globalDoc);
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            scores.put(ord, score);
          }
        } else {
          // test this ord to see if it's a new group leader
          if (compareState.testAndSetGroupValues(ord, contextDoc)) {
            ords.put(ord, globalDoc);
            if (needsScores) {
              if (!needsScores4Collapsing) {
                this.score = scorer.score();
              }
              scores.put(ord, score);
            }
          }
        }
      } else if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (-1 == nullDoc) {
          // we've never seen a doc with null collapse key yet, treat it as the null group head for
          // now
          compareState.setNullGroupValues(contextDoc);
          nullDoc = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            this.nullScore = score;
          }
        } else {
          // test this doc to see if it's the new null leader
          if (compareState.testAndSetNullGroupValues(contextDoc)) {
            nullDoc = globalDoc;
            if (needsScores) {
              if (!needsScores4Collapsing) {
                this.score = scorer.score();
              }
              this.nullScore = score;
            }
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            this.score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  /** Builder for {@link IntFieldValueCollector}. */
  public record IntFieldCollectorBuilder(
      GroupHeadSelector groupHeadSelector,
      NullPolicy nullPolicy,
      String collapseField,
      int initialSize,
      boolean needsScores4Collapsing,
      IntIntHashMap boostDocsMap,
      IndexSearcher searcher) {

    public DelegatingCollector build(
        SortSpec sortSpec, FieldType fieldType, FunctionQuery funcQuery) throws IOException {
      if (null != sortSpec) {
        return new IntSortSpecCollector(this, sortSpec);
      } else if (funcQuery != null) {
        return new IntValueSourceCollector(this, funcQuery);
      } else {
        NumberType numType = fieldType.getNumberType();
        assert null != numType; // shouldn't make it here for non-numeric types
        return switch (numType) {
          case INTEGER -> new IntIntCollector(this);
          case FLOAT -> new IntFloatCollector(this);
          default -> throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "min/max must be Int or Float field types when collapsing on numeric fields");
        };
      }
    }
  }

  /**
   * Collapses on an integer field.
   *
   * @lucene.internal
   */
  protected abstract static class IntFieldValueCollector extends AbstractCollapseCollector {
    // Configuration
    protected final String collapseField;
    protected final int initialSize; // data structure size hint

    // Source data
    protected NumericDocValues collapseValues;

    // Results/accumulator
    protected final IntIntHashMap cmap;
    protected final IntIntDynamicMap docs;
    protected IntFloatDynamicMap scores;

    protected IntFieldValueCollector(IntFieldCollectorBuilder ctx) throws IOException {
      super(ctx.searcher, ctx.nullPolicy, ctx.needsScores4Collapsing, ctx.boostDocsMap);

      assert !GroupHeadSelectorType.SCORE.equals(ctx.groupHeadSelector.type);

      this.collapseField = ctx.collapseField;
      this.initialSize = ctx.initialSize;

      this.cmap = new IntIntHashMap(ctx.initialSize);
      this.docs = new IntIntDynamicMap(ctx.initialSize, 0);
    }

    @Override
    protected void initializeCollection(boolean needsScores) {
      super.initializeCollection(needsScores);
      if (needsScores) {
        this.scores = new IntFloatDynamicMap(initialSize, 0.0f);
      }
    }

    protected abstract void collapseNullGroup(int contextDoc, int globalDoc) throws IOException;

    protected abstract void collapse(int collapseKey, int contextDoc, int globalDoc)
        throws IOException;

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.collapseValues = DocValues.getNumeric(context.reader(), this.collapseField);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      final int globalDoc = contextDoc + this.docBase;
      if (collapseValues.advanceExact(contextDoc)) {
        final int collapseKey = (int) collapseValues.longValue();
        // Check to see if we have documents boosted by the QueryElevationComponent
        if (boostedDocsCollector.collectIfBoosted(collapseKey, globalDoc)) return;
        collapse(collapseKey, contextDoc, globalDoc);

      } else { // Null Group...
        // Check to see if we have documents boosted by the QueryElevationComponent
        if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        if (NullPolicy.IGNORE != nullPolicy) {
          collapseNullGroup(contextDoc, globalDoc);
        }
      }
    }

    @Override
    protected void finishCollapsedSet() {
      boostedDocsCollector.visitBoostedGroupKeys(cmap::remove);

      DocIdSetBuilder.BulkAdder adder = collapsedSet.grow(cmap.size());
      for (IntIntCursor cursor : cmap) {
        int pointer = cursor.value;
        adder.add(docs.get(pointer));
      }
    }

    @Override
    protected Scorable getCollapsedScores(DocIdSetIterator disi, LeafReaderContext context)
        throws IOException {
      this.collapseValues = DocValues.getNumeric(context.reader(), this.collapseField);
      return new CachedScoreScorable(disi) {
        @Override
        protected float computeScore(int globalDoc) throws IOException {
          int contextDoc = globalDoc - context.docBase;
          if (collapseValues.advanceExact(contextDoc)) {
            final int collapseValue = (int) collapseValues.longValue();
            final int pointer = cmap.get(collapseValue);
            return scores.get(pointer);
          }
          return scoreNullGroup(globalDoc);
        }
      };
    }
  }

  /**
   * Collector for collapsing on int field using min/max of an integer field to select group head.
   */
  protected static class IntIntCollector extends IntFieldValueCollector {
    // Configuration
    private final String field;
    private final IntCompare comp;

    // Source data
    private NumericDocValues minMaxVals;

    // Results/accumulator
    private final IntIntDynamicMap testValues;
    private int nullCompVal;
    private int index = -1;

    public IntIntCollector(IntFieldCollectorBuilder ctx) throws IOException {
      super(ctx);
      this.field = ctx.groupHeadSelector.selectorText;
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxIntComp();
        this.nullCompVal = Integer.MIN_VALUE;
      } else {
        comp = new MinIntComp();
        this.nullCompVal = Integer.MAX_VALUE;
      }

      this.testValues = new IntIntDynamicMap(ctx.initialSize, 0);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    private int advanceAndGetCurrentVal(int contextDoc) throws IOException {
      if (minMaxVals.advanceExact(contextDoc)) {
        return (int) minMaxVals.longValue();
      }
      return 0;
    }

    @Override
    protected void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      final int currentVal = advanceAndGetCurrentVal(contextDoc);

      final int idx;
      if ((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if (comp.test(currentVal, testValues.get(pointer))) {
          testValues.put(pointer, currentVal);
          docs.put(pointer, globalDoc);
          if (needsScores) {
            scores.put(pointer, scorer.score());
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        testValues.put(index, currentVal);
        docs.put(index, globalDoc);
        if (needsScores) {
          scores.put(index, scorer.score());
        }
      }
    }

    @Override
    protected void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE != this.nullPolicy;

      final int currentVal = advanceAndGetCurrentVal(contextDoc);
      if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  /** Collector for collapsing on int field using min/max of a float field to select group head. */
  protected static class IntFloatCollector extends IntFieldValueCollector {
    // Configuration
    private final String field;
    private final FloatCompare comp;

    // Source data
    private NumericDocValues minMaxVals;

    // Results/accumulator
    private final IntFloatDynamicMap testValues;
    private float nullCompVal;
    private int index = -1;

    public IntFloatCollector(IntFieldCollectorBuilder ctx) throws IOException {
      super(ctx);
      this.field = ctx.groupHeadSelector.selectorText;
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        this.nullCompVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        this.nullCompVal = Float.MAX_VALUE;
      }

      this.testValues = new IntFloatDynamicMap(ctx.initialSize, 0.0f);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    private float advanceAndGetCurrentVal(int contextDoc) throws IOException {
      if (minMaxVals.advanceExact(contextDoc)) {
        return Float.intBitsToFloat((int) minMaxVals.longValue());
      }
      return 0.0f;
    }

    @Override
    protected void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      final float currentVal = advanceAndGetCurrentVal(contextDoc);

      final int idx;
      if ((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if (comp.test(currentVal, testValues.get(pointer))) {
          testValues.put(pointer, currentVal);
          docs.put(pointer, globalDoc);
          if (needsScores) {
            scores.put(pointer, scorer.score());
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        testValues.put(index, currentVal);
        docs.put(index, globalDoc);
        if (needsScores) {
          scores.put(index, scorer.score());
        }
      }
    }

    @Override
    protected void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE != this.nullPolicy;
      final float currentVal = advanceAndGetCurrentVal(contextDoc);
      if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  /** Collector for collapsing on int field using function query to select group head. */
  protected static class IntValueSourceCollector extends IntFieldValueCollector {
    // Configuration
    private final FloatCompare comp;
    private final ValueSource valueSource;
    private final Map<Object, Object> rcontext;

    // Source data
    private FunctionValues functionValues;

    // Results/accumulator
    private final CollapseScore collapseScore = new CollapseScore();
    private final IntFloatDynamicMap testValues;
    private float nullCompVal;
    private int index = -1;

    public IntValueSourceCollector(IntFieldCollectorBuilder ctx, FunctionQuery funcQuery)
        throws IOException {
      super(ctx);
      assert GroupHeadSelectorType.MIN_MAX.contains(ctx.groupHeadSelector.type);
      if (GroupHeadSelectorType.MAX.equals(ctx.groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        nullCompVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        nullCompVal = Float.MAX_VALUE;
      }
      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(ctx.searcher);

      collapseScore.setupIfNeeded(ctx.groupHeadSelector, rcontext);

      this.testValues = new IntFloatDynamicMap(ctx.initialSize, 0.0f);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      functionValues = this.valueSource.getValues(rcontext, context);
    }

    private float computeScoreIfNeeded4Collapse() throws IOException {
      if (needsScores4Collapsing) {
        this.collapseScore.score = scorer.score();
        return this.collapseScore.score;
      }
      return 0F;
    }

    @Override
    protected void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      float score = computeScoreIfNeeded4Collapse();
      final float currentVal = functionValues.floatVal(contextDoc);

      final int idx;
      if ((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if (comp.test(currentVal, testValues.get(pointer))) {
          testValues.put(pointer, currentVal);
          docs.put(pointer, globalDoc);
          if (needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores.put(pointer, score);
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        docs.put(index, globalDoc);
        testValues.put(index, currentVal);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          scores.put(index, score);
        }
      }
    }

    @Override
    protected void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE != this.nullPolicy;

      float score = computeScoreIfNeeded4Collapse();
      final float currentVal = functionValues.floatVal(contextDoc);

      if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
          nullDoc = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            nullScore = score;
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  /** Collector for collapsing on int field using a sort spec to select group head. */
  protected static class IntSortSpecCollector extends IntFieldValueCollector {
    // Configuration
    private final SortFieldsCompare compareState;

    // Results/accumulator
    private float score;
    private int index = -1;

    public IntSortSpecCollector(IntFieldCollectorBuilder ctx, SortSpec sortSpec)
        throws IOException {
      super(ctx);

      assert GroupHeadSelectorType.SORT.equals(ctx.groupHeadSelector.type);

      Sort sort = rewriteSort(sortSpec, ctx.searcher);
      this.compareState = new SortFieldsCompare(sort.getSort(), ctx.initialSize);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      compareState.setNextReader(context);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      super.setScorer(scorer);
      this.compareState.setScorer(scorer);
    }

    private float computeScoreIfNeeded4Collapse() throws IOException {
      if (needsScores4Collapsing) {
        return scorer.score();
      }
      return 0f;
    }

    @Override
    protected void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      score = computeScoreIfNeeded4Collapse();

      final int idx;
      if ((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if (compareState.testAndSetGroupValues(pointer, contextDoc)) {
          docs.put(pointer, globalDoc);
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            scores.put(pointer, score);
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        docs.put(index, globalDoc);
        compareState.setGroupValues(index, contextDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            this.score = scorer.score();
          }
          scores.put(index, score);
        }
      }
    }

    @Override
    protected void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE != this.nullPolicy;

      score = computeScoreIfNeeded4Collapse();

      if (this.nullPolicy == NullPolicy.COLLAPSE) {
        if (-1 == nullDoc) {
          compareState.setNullGroupValues(contextDoc);
          nullDoc = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            nullScore = score;
          }
        } else {
          if (compareState.testAndSetNullGroupValues(contextDoc)) {
            nullDoc = globalDoc;
            if (needsScores) {
              if (!needsScores4Collapsing) {
                this.score = scorer.score();
              }
              nullScore = score;
            }
          }
        }
      } else if (this.nullPolicy == NullPolicy.EXPAND) {
        this.collapsedSet.grow(1).add(globalDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            this.score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  /**
   * Base class for collectors that will do collapsing using "block indexed" documents
   *
   * @lucene.internal
   */
  private abstract static class AbstractBlockCollector extends DelegatingCollector {

    // Configuration
    protected final String collapseField;
    protected final boolean needsScores4Collapsing;
    protected final boolean expandNulls;

    // Results/accumulator
    protected final BlockGroupState currentGroupState = new BlockGroupState();
    private final MergeBoost boostDocs;

    protected AbstractBlockCollector(
        final String collapseField,
        final NullPolicy nullPolicy,
        final IntIntHashMap boostDocsMap,
        final boolean needsScores4Collapsing) {

      this.collapseField = collapseField;
      this.needsScores4Collapsing = needsScores4Collapsing;

      assert nullPolicy == NullPolicy.IGNORE || nullPolicy == NullPolicy.EXPAND;
      this.expandNulls = (NullPolicy.EXPAND == nullPolicy);
      this.boostDocs = BoostedDocsCollector.build(boostDocsMap).getMergeBoost();

      currentGroupState.resetForNewGroup();
    }

    @Override
    public ScoreMode scoreMode() {
      return needsScores4Collapsing ? ScoreMode.COMPLETE : super.scoreMode();
    }

    /** If we have a candidate match, delegate the collection of that match. */
    protected void maybeDelegateCollect() throws IOException {
      if (currentGroupState.isCurrentDocCollectable()) {
        delegateCollect();
      }
    }

    /** Immediately delegate the collection of the current doc */
    protected void delegateCollect() throws IOException {
      // ensure we have the 'correct' scorer
      // (our supper class may have set the "real" scorer on our leafDelegate
      // and it may have an incorrect docID)
      leafDelegate.setScorer(currentGroupState);
      leafDelegate.collect(currentGroupState.docId);
    }

    /**
     * NOTE: collects the best doc for the last group in the previous segment subclasses must call
     * super <em>BEFORE</em> they make any changes to their own state that might influence
     * collection
     */
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      maybeDelegateCollect();
      // Now setup for the next segment.
      currentGroupState.resetForNewGroup();
      this.docBase = context.docBase;
      super.doSetNextReader(context);
    }

    /**
     * Acts as an id iterator over the boosted docs
     *
     * @param contextDoc the context specific docId to check for, iterator is advanced to this id
     * @return true if the contextDoc is boosted, false otherwise.
     */
    protected boolean isBoostedAdvanceExact(final int contextDoc) {
      return boostDocs.boost(contextDoc + docBase);
    }

    @Override
    public void complete() throws IOException {
      // Deal with last group (if any)...
      maybeDelegateCollect();

      super.complete();
    }

    /**
     * Encapsulates basic state information about the current group, and the "best matching"
     * document in that group (so far)
     */
    protected static final class BlockGroupState extends ScoreAndDoc {
      /**
       * Specific values have no intrinsic meaning, but can <em>only</em> be considered if the
       * current docID is non-negative
       */
      private int currentGroup = 0;

      private boolean groupHasBoostedDocs;

      public void setCurrentGroup(final int groupId) {
        this.currentGroup = groupId;
      }

      public int getCurrentGroup() {
        assert -1 < this.docId;
        return this.currentGroup;
      }

      public void setBestDocForCurrentGroup(final int contextDoc, final boolean isBoosted) {
        this.docId = contextDoc;
        this.groupHasBoostedDocs |= isBoosted;
      }

      public void resetForNewGroup() {
        this.docId = -1;
        this.score = Float.MIN_VALUE;
        this.groupHasBoostedDocs = false;
      }

      public boolean hasBoostedDocs() {
        assert -1 < this.docId;
        return groupHasBoostedDocs;
      }

      /**
       * Returns true if we have a valid ("best match") docId for the current group and there are no
       * boosted docs for this group (If the current doc was boosted, it should have already been
       * collected)
       */
      public boolean isCurrentDocCollectable() {
        return (-1 < this.docId && !groupHasBoostedDocs);
      }
    }
  }

  /**
   * Collapses groups on a block using a field that has values unique to that block (example: <code>
   * _root_</code>) choosing the group head based on score
   *
   * @lucene.internal
   */
  abstract static class AbstractBlockScoreCollector extends AbstractBlockCollector {

    public AbstractBlockScoreCollector(
        final String collapseField, final NullPolicy nullPolicy, final IntIntHashMap boostDocsMap) {
      super(collapseField, nullPolicy, boostDocsMap, true);
    }

    private void setCurrentGroupBestMatch(
        final int contextDocId, final float score, final boolean isBoosted) {
      currentGroupState.setBestDocForCurrentGroup(contextDocId, isBoosted);
      currentGroupState.score = score;
    }

    /**
     * This method should be called by subclasses for each doc + group encountered
     *
     * @param contextDoc a valid doc id relative to the current reader context
     * @param docGroup some uique identifier for the group - the base class makes no assumptions
     *     about it's meaning
     * @see #collectDocWithNullGroup
     */
    protected void collectDocWithGroup(int contextDoc, int docGroup) throws IOException {
      assert 0 <= contextDoc;

      final boolean isBoosted = isBoostedAdvanceExact(contextDoc);

      if (-1 < currentGroupState.docId && docGroup == currentGroupState.getCurrentGroup()) {
        // we have an existing group, and contextDoc is in that group.

        if (isBoosted) {
          // this doc is the best and should be immediately collected regardless of score
          setCurrentGroupBestMatch(contextDoc, scorer.score(), isBoosted);
          delegateCollect();

        } else if (currentGroupState.hasBoostedDocs()) {
          // No-Op: nothing about this doc matters since we've already collected boosted docs in
          // this group

          // No-Op
        } else {
          // check if this doc the new 'best' doc in this group...
          final float score = scorer.score();
          if (score > currentGroupState.score) {
            setCurrentGroupBestMatch(contextDoc, scorer.score(), isBoosted);
          }
        }

      } else {
        // We have a document that starts a new group (or may be the first doc+group we've collected
        // this segment)

        // first collect the prior group if needed...
        maybeDelegateCollect();

        // then setup the new group and current best match
        currentGroupState.resetForNewGroup();
        currentGroupState.setCurrentGroup(docGroup);
        setCurrentGroupBestMatch(contextDoc, scorer.score(), isBoosted);

        if (isBoosted) { // collect immediately
          delegateCollect();
        }
      }
    }

    /**
     * This method should be called by subclasses for each doc encountered that is not in a group
     * (ie: null group)
     *
     * @param contextDoc a valid doc id relative to the current reader context
     * @see #collectDocWithGroup
     */
    protected void collectDocWithNullGroup(int contextDoc) throws IOException {
      assert 0 <= contextDoc;

      // NOTE: with 'null group' docs, it doesn't matter if they are boosted since we don't suppor
      // collapsing nulls

      // this doc is definitely not part of any prior group, so collect if needed...
      maybeDelegateCollect();

      if (expandNulls) {
        // set & immediately collect our current doc...
        setCurrentGroupBestMatch(contextDoc, scorer.score(), false);
        delegateCollect();

      } else {
        // we're ignoring nulls, so: No-Op.
      }

      // either way re-set for the next doc / group
      currentGroupState.resetForNewGroup();
    }
  }

  /**
   * A block based score collector that uses a field's "ord" as the group ids
   *
   * @lucene.internal
   */
  static class BlockOrdScoreCollector extends AbstractBlockScoreCollector {
    private SortedDocValues segmentValues;

    public BlockOrdScoreCollector(
        final String collapseField, final NullPolicy nullPolicy, final IntIntHashMap boostDocsMap)
        throws IOException {
      super(collapseField, nullPolicy, boostDocsMap);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getSorted(context.reader(), collapseField);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int ord = segmentValues.ordValue();
        collectDocWithGroup(contextDoc, ord);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }

  /**
   * A block based score collector that uses a field's numeric value as the group ids
   *
   * @lucene.internal
   */
  static class BlockIntScoreCollector extends AbstractBlockScoreCollector {
    private NumericDocValues segmentValues;

    public BlockIntScoreCollector(
        final String collapseField, final NullPolicy nullPolicy, final IntIntHashMap boostDocsMap)
        throws IOException {
      super(collapseField, nullPolicy, boostDocsMap);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getNumeric(context.reader(), collapseField);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int group = (int) segmentValues.longValue();
        collectDocWithGroup(contextDoc, group);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }

  /**
   * Collapses groups on a block using a field that has values unique to that block (example: <code>
   * _root_</code>) choosing the group head based on a {@link SortSpec} (which can be synthetically
   * created for min/max group head selectors using {@link #getSort})
   *
   * <p>Note that since this collector does a single pass, and unlike other collectors doesn't need
   * to maintain a large data structure of scores (for all matching docs) when they might be needed
   * for the response, it has no need to distinguish between the concepts of <code>
   * needsScores4Collapsing</code> vs </code>needsScores</code>
   *
   * @lucene.internal
   */
  abstract static class AbstractBlockSortSpecCollector extends AbstractBlockCollector {

    /**
     * Helper method for extracting a {@link Sort} out of a {@link SortSpec} <em>or</em> creating
     * one synthetically for "min/max" {@link GroupHeadSelector} against a {@link FunctionQuery}
     * <em>or</em> simple field name.
     *
     * @return appropriate (already re-written) Sort to use with a AbstractBlockSortSpecCollector
     */
    public static Sort getSort(
        final GroupHeadSelector groupHeadSelector,
        final SortSpec sortSpec,
        final FunctionQuery funcQuery,
        final SolrIndexSearcher searcher)
        throws IOException {
      if (null != sortSpec) {
        assert GroupHeadSelectorType.SORT.equals(groupHeadSelector.type);

        // a "feature" of SortSpec is that getSort() is null if we're just using 'score desc'
        if (null == sortSpec.getSort()) {
          return Sort.RELEVANCE.rewrite(searcher);
        }
        return sortSpec.getSort().rewrite(searcher);
      } // else: min/max on field or value source...

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      assert !CollapseScore.wantsCScore(groupHeadSelector.selectorText);

      final boolean reverse = GroupHeadSelectorType.MAX.equals(groupHeadSelector.type);
      final SortField sf =
          (null != funcQuery)
              ? funcQuery.getValueSource().getSortField(reverse)
              : searcher.getSchema().getField(groupHeadSelector.selectorText).getSortField(reverse);

      return (new Sort(sf)).rewrite(searcher);
    }

    private final BlockBasedSortFieldsCompare sortsCompare;

    public AbstractBlockSortSpecCollector(
        final String collapseField,
        final NullPolicy nullPolicy,
        final IntIntHashMap boostDocsMap,
        final Sort sort) {
      super(collapseField, nullPolicy, boostDocsMap, sort.needsScores());
      this.sortsCompare = new BlockBasedSortFieldsCompare(sort.getSort());
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      super.setScorer(scorer);
      sortsCompare.setScorer(scorer);
    }

    private void setCurrentGroupBestMatch(final int contextDocId, final boolean isBoosted)
        throws IOException {
      currentGroupState.setBestDocForCurrentGroup(contextDocId, isBoosted);
      if (scoreMode().needsScores()) {
        currentGroupState.score = scorer.score();
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.sortsCompare.setNextReader(context);
    }

    /**
     * This method should be called by subclasses for each doc + group encountered
     *
     * @param contextDoc a valid doc id relative to the current reader context
     * @param docGroup some uique identifier for the group - the base class makes no assumptions
     *     about it's meaning
     * @see #collectDocWithNullGroup
     */
    protected void collectDocWithGroup(int contextDoc, int docGroup) throws IOException {
      assert 0 <= contextDoc;

      final boolean isBoosted = isBoostedAdvanceExact(contextDoc);

      if (-1 < currentGroupState.docId && docGroup == currentGroupState.getCurrentGroup()) {
        // we have an existing group, and contextDoc is in that group.

        if (isBoosted) {
          // this doc is the best and should be immediately collected regardless of sort values
          setCurrentGroupBestMatch(contextDoc, isBoosted);
          delegateCollect();

        } else if (currentGroupState.hasBoostedDocs()) {
          // No-Op: nothing about this doc matters since we've already collected boosted docs in
          // this group

          // No-Op
        } else {
          // check if it's the new 'best' doc in this group...
          if (sortsCompare.testAndSetGroupValues(contextDoc)) {
            setCurrentGroupBestMatch(contextDoc, isBoosted);
          }
        }

      } else {
        // We have a document that starts a new group (or may be the first doc+group we've collected
        // this segmen)

        // first collect the prior group if needed...
        maybeDelegateCollect();

        // then setup the new group and current best match
        currentGroupState.resetForNewGroup();
        currentGroupState.setCurrentGroup(docGroup);
        sortsCompare.setGroupValues(contextDoc);
        setCurrentGroupBestMatch(contextDoc, isBoosted);

        if (isBoosted) { // collect immediately
          delegateCollect();
        }
      }
    }

    /**
     * This method should be called by subclasses for each doc encountered that is not in a group
     * (ie: null group)
     *
     * @param contextDoc a valid doc id relative to the current reader context
     * @see #collectDocWithGroup
     */
    protected void collectDocWithNullGroup(int contextDoc) throws IOException {
      assert 0 <= contextDoc;

      // NOTE: with 'null group' docs, it doesn't matter if they are boosted since we don't suppor
      // collapsing nulls

      // this doc is definitely not part of any prior group, so collect if needed...
      maybeDelegateCollect();

      if (expandNulls) {
        // set & immediately collect our current doc...
        setCurrentGroupBestMatch(contextDoc, false);
        // NOTE: sort values don't matter
        delegateCollect();

      } else {
        // we're ignoring nulls, so: No-Op.
      }

      // either way re-set for the next doc / group
      currentGroupState.resetForNewGroup();
    }
  }

  /**
   * A block based score collector that uses a field's "ord" as the group ids
   *
   * @lucene.internal
   */
  static class BlockOrdSortSpecCollector extends AbstractBlockSortSpecCollector {
    private SortedDocValues segmentValues;

    public BlockOrdSortSpecCollector(
        final String collapseField,
        final NullPolicy nullPolicy,
        final IntIntHashMap boostDocsMap,
        final Sort sort)
        throws IOException {
      super(collapseField, nullPolicy, boostDocsMap, sort);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getSorted(context.reader(), collapseField);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int ord = segmentValues.ordValue();
        collectDocWithGroup(contextDoc, ord);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }

  /**
   * A block based score collector that uses a field's numeric value as the group ids
   *
   * @lucene.internal
   */
  static class BlockIntSortSpecCollector extends AbstractBlockSortSpecCollector {
    private NumericDocValues segmentValues;

    public BlockIntSortSpecCollector(
        final String collapseField,
        final NullPolicy nullPolicy,
        final IntIntHashMap boostDocsMap,
        final Sort sort)
        throws IOException {
      super(collapseField, nullPolicy, boostDocsMap, sort);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getNumeric(context.reader(), collapseField);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int group = (int) segmentValues.longValue();
        collectDocWithGroup(contextDoc, group);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }

  public static final class CollapseScore {
    /**
     * Inspects the GroupHeadSelector to determine if this CollapseScore is needed. If it is, then
     * "this" will be added to the readerContext using the "CSCORE" key, and true will be returned.
     * If not returns false.
     */
    public boolean setupIfNeeded(
        final GroupHeadSelector groupHeadSelector,
        final Map<? super String, ? super CollapseScore> readerContext) {
      // HACK, but not really any better options until/unless we can recursively
      // ask value sources if they depend on score
      if (wantsCScore(groupHeadSelector.selectorText)) {
        readerContext.put("CSCORE", this);
        return true;
      }
      return false;
    }

    /**
     * Huge HACK, but not really any better options until/unless we can recursively ask value
     * sources if they depend on score
     */
    public static boolean wantsCScore(final String text) {
      return (text.contains("cscore()"));
    }

    private CollapseScore() {
      // No-Op
    }

    public float score;
  }

  /**
   * Helper class for dealing with boosted docs, which always get collected (even if there is more
   * then one in a group) and suppress any non-boosted docs from being collected from their group
   * (even if they should be based on the group head selectors)
   *
   * <p>NOTE: collect methods must be called in increasing globalDoc order
   */
  protected static class BoostedDocsCollector {
    private final int[] sortedGlobalDocIds;
    private final boolean hasBoosts;

    private final IntArrayList boostedKeys = new IntArrayList();
    private final IntArrayList boostedDocs = new IntArrayList();

    private boolean boostedNullGroup = false;
    private final MergeBoost boostedDocsIdsIter;

    public static BoostedDocsCollector build(final IntIntHashMap boostDocsMap) {
      if (null != boostDocsMap && !boostDocsMap.isEmpty()) {
        return new BoostedDocsCollector(boostDocsMap);
      }

      // else: No-Op impl (short circut default impl)....
      return new BoostedDocsCollector(new IntIntHashMap()) {
        @Override
        public boolean collectIfBoosted(int groupKey, int globalDoc) {
          return false;
        }

        @Override
        public boolean collectInNullGroupIfBoosted(int globalDoc) {
          return false;
        }
      };
    }

    private BoostedDocsCollector(final IntIntHashMap boostDocsMap) {
      this.hasBoosts = !boostDocsMap.isEmpty();
      sortedGlobalDocIds = new int[boostDocsMap.size()];
      Iterator<IntIntCursor> it = boostDocsMap.iterator();
      int index = -1;
      while (it.hasNext()) {
        IntIntCursor cursor = it.next();
        sortedGlobalDocIds[++index] = cursor.key;
      }

      Arrays.sort(sortedGlobalDocIds);
      boostedDocsIdsIter = getMergeBoost();
    }

    /** True if there are any requested boosts (regardless of whether any have been collected) */
    public boolean hasBoosts() {
      return hasBoosts;
    }

    /** Returns a brand new MergeBoost instance listing all requested boosted docs */
    public MergeBoost getMergeBoost() {
      return new MergeBoost(sortedGlobalDocIds);
    }

    /**
     * @return true if doc is boosted and has (now) been collected
     */
    public boolean collectIfBoosted(int groupKey, int globalDoc) {
      if (boostedDocsIdsIter.boost(globalDoc)) {
        this.boostedDocs.add(globalDoc);
        this.boostedKeys.add(groupKey);
        return true;
      }
      return false;
    }

    /**
     * @return true if doc is boosted and has (now) been collected
     */
    public boolean collectInNullGroupIfBoosted(int globalDoc) {
      if (boostedDocsIdsIter.boost(globalDoc)) {
        this.boostedDocs.add(globalDoc);
        this.boostedNullGroup = true;
        return true;
      }
      return false;
    }

    // REMAINING METHODS ARE USED IN COMPLETE() TO CONSUME

    /** Add all collected boosted docs to the collapsed set. */
    public void addBoostedDocsTo(DocIdSetBuilder collapsedSet) {
      DocIdSetBuilder.BulkAdder adder = collapsedSet.grow(boostedDocs.size());
      boostedDocs.forEach((IntProcedure) adder::add);
    }

    /** Visit group keys that have boosted docs */
    public void visitBoostedGroupKeys(IntProcedure procedure) {
      boostedKeys.forEach(procedure);
    }

    /**
     * Whether a boosted doc was collected in the null group. If true, the null group head should be
     * reset so the boosted doc takes precedence.
     */
    public boolean isBoostedNullGroup() {
      return boostedNullGroup;
    }
  }

  protected static class MergeBoost {

    private int[] boostDocs;
    private int index = 0;

    public MergeBoost(int[] boostDocs) {
      this.boostDocs = boostDocs;
    }

    public void reset() {
      this.index = 0;
    }

    public boolean boost(int globalDoc) {
      if (index == Integer.MIN_VALUE) {
        return false;
      } else {
        while (true) {
          if (index >= boostDocs.length) {
            index = Integer.MIN_VALUE;
            return false;
          } else {
            int comp = boostDocs[index];
            if (comp == globalDoc) {
              ++index;
              return true;
            } else if (comp < globalDoc) {
              ++index;
            } else {
              return false;
            }
          }
        }
      }
    }
  }

  /**
   * This structure wraps (and semi-emulates) the {@link SortFieldsCompare} functionality/API for
   * "block" based group collection, where we only ever need a single group in memory at a time As a
   * result, it's API has a smaller surface area...
   */
  private static class BlockBasedSortFieldsCompare {
    /**
     * this will always have a numGroups of '0' and we will (ab)use the 'null' group methods for
     * tracking and comparison as we collect docs (since we only ever consider one group at a time)
     */
    private final SortFieldsCompare inner;

    public BlockBasedSortFieldsCompare(final SortField[] sorts) {
      this.inner = new SortFieldsCompare(sorts, 0);
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      inner.setNextReader(context);
    }

    public void setScorer(Scorable s) throws IOException {
      inner.setScorer(s);
    }

    /**
     * @see SortFieldsCompare#setGroupValues
     */
    public void setGroupValues(int contextDoc) throws IOException {
      inner.setNullGroupValues(contextDoc);
    }

    /**
     * @see SortFieldsCompare#testAndSetGroupValues
     */
    public boolean testAndSetGroupValues(int contextDoc) throws IOException {
      return inner.testAndSetNullGroupValues(contextDoc);
    }
  }

  /**
   * Class for comparing documents according to a list of SortField clauses and tracking the
   * groupHeadLeaders and their sort values. groups will be identified by int "contextKey values,
   * which may either be (encoded) 32bit numeric values, or ordinal values for Strings -- this class
   * doesn't care, and doesn't assume any special meaning.
   */
  private static class SortFieldsCompare {
    private final int numClauses;
    private final SortField[] sorts;
    private final int[] reverseMul;

    @SuppressWarnings({"rawtypes"})
    private final FieldComparator[] fieldComparators;

    private final LeafFieldComparator[] leafFieldComparators;

    private Object[][] groupHeadValues; // growable
    private final Object[] nullGroupValues;

    /**
     * Constructs an instance based on the (raw, un-rewritten) SortFields to be used, and an initial
     * number of expected groups (will grow as needed).
     */
    @SuppressWarnings({"rawtypes"})
    public SortFieldsCompare(SortField[] sorts, int initNumGroups) {
      this.sorts = sorts;
      numClauses = sorts.length;
      fieldComparators = new FieldComparator[numClauses];
      leafFieldComparators = new LeafFieldComparator[numClauses];
      reverseMul = new int[numClauses];
      for (int clause = 0; clause < numClauses; clause++) {
        SortField sf = sorts[clause];
        // we only need one slot for every comparator
        fieldComparators[clause] =
            sf.getComparator(
                1,
                clause == 0
                    ? (numClauses > 1 ? Pruning.GREATER_THAN : Pruning.GREATER_THAN_OR_EQUAL_TO)
                    : Pruning.NONE);

        reverseMul[clause] = sf.getReverse() ? -1 : 1;
      }
      groupHeadValues = new Object[initNumGroups][];
      nullGroupValues = new Object[numClauses];
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      for (int clause = 0; clause < numClauses; clause++) {
        leafFieldComparators[clause] = fieldComparators[clause].getLeafComparator(context);
      }
    }

    public void setScorer(Scorable s) throws IOException {
      for (int clause = 0; clause < numClauses; clause++) {
        leafFieldComparators[clause].setScorer(s);
      }
    }

    // LUCENE-6808 workaround
    private static Object cloneIfBytesRef(Object val) {
      if (val instanceof BytesRef) {
        return BytesRef.deepCopyOf((BytesRef) val);
      }
      return val;
    }

    /**
     * Returns the current SortField values for the specified collapseKey. If this collapseKey has
     * never been seen before, then an array of null values is inited and tracked so that the caller
     * may update it if needed.
     */
    private Object[] getOrInitGroupHeadValues(int collapseKey) {
      Object[] values = groupHeadValues[collapseKey];
      if (null == values) {
        values = new Object[numClauses];
        groupHeadValues[collapseKey] = values;
      }
      return values;
    }

    /**
     * Records the SortField values for the specified contextDoc as the "best" values for the group
     * identified by the specified collapseKey.
     *
     * <p>Should be called the first time a contextKey is encountered.
     */
    public void setGroupValues(int collapseKey, int contextDoc) throws IOException {
      assert 0 <= collapseKey : "negative collapseKey";
      if (collapseKey >= groupHeadValues.length) {
        grow(collapseKey + 1);
      }
      setGroupValues(getOrInitGroupHeadValues(collapseKey), contextDoc);
    }

    /**
     * Records the SortField values for the specified contextDoc as the "best" values for the null
     * group.
     *
     * <p>Should be calledthe first time a doc in the null group is encountered
     */
    public void setNullGroupValues(int contextDoc) throws IOException {
      setGroupValues(nullGroupValues, contextDoc);
    }

    /**
     * Records the SortField values for the specified contextDoc into the values array provided by
     * the caller.
     */
    private void setGroupValues(Object[] values, int contextDoc) throws IOException {
      for (int clause = 0; clause < numClauses; clause++) {
        leafFieldComparators[clause].copy(0, contextDoc);
        values[clause] = cloneIfBytesRef(fieldComparators[clause].value(0));
      }
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing group head values
     * for the group identified by the specified collapseKey, and overwrites them (and returns true)
     * if this document should become the new group head in accordance with the SortFields
     * (otherwise returns false)
     */
    public boolean testAndSetGroupValues(int collapseKey, int contextDoc) throws IOException {
      assert 0 <= collapseKey : "negative collapseKey";
      if (collapseKey >= groupHeadValues.length) {
        grow(collapseKey + 1);
      }
      return testAndSetGroupValues(getOrInitGroupHeadValues(collapseKey), contextDoc);
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing group head values
     * for the null group, and overwrites them (and returns true) if this document should become the
     * new group head in accordance with the SortFields. (otherwise returns false)
     */
    public boolean testAndSetNullGroupValues(int contextDoc) throws IOException {
      return testAndSetGroupValues(nullGroupValues, contextDoc);
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing values array, and
     * overwrites them (and returns true) if this document is the new group head in accordance with
     * the SortFields. (otherwise returns false)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean testAndSetGroupValues(Object[] values, int contextDoc) throws IOException {
      Object[] stash = new Object[numClauses];
      int lastCompare = 0;
      int testClause = 0;
      for (
      /* testClause */ ; testClause < numClauses; testClause++) {
        leafFieldComparators[testClause].copy(0, contextDoc);
        FieldComparator fcomp = fieldComparators[testClause];
        stash[testClause] = cloneIfBytesRef(fcomp.value(0));
        lastCompare =
            reverseMul[testClause] * fcomp.compareValues(stash[testClause], values[testClause]);

        if (0 != lastCompare) {
          // no need to keep checking additional clauses
          break;
        }
      }

      if (0 <= lastCompare) {
        // we're either not competitive, or we're completely tied with another doc that's already
        // group head that's already been selected
        return false;
      } // else...

      // this doc is our new group head, we've already read some of the values into our stash
      testClause++;
      System.arraycopy(stash, 0, values, 0, testClause);
      // read the remaining values we didn't need to test
      for (int copyClause = testClause; copyClause < numClauses; copyClause++) {
        leafFieldComparators[copyClause].copy(0, contextDoc);
        values[copyClause] = cloneIfBytesRef(fieldComparators[copyClause].value(0));
      }
      return true;
    }

    /** Grows all internal arrays to the specified minSize */
    public void grow(int minSize) {
      groupHeadValues = ArrayUtil.grow(groupHeadValues, minSize);
    }
  }

  private static interface IntCompare {
    public boolean test(int i1, int i2);
  }

  private static interface FloatCompare {
    public boolean test(float i1, float i2);
  }

  private static interface LongCompare {
    public boolean test(long i1, long i2);
  }

  private static class MaxIntComp implements IntCompare {
    @Override
    public boolean test(int i1, int i2) {
      return i1 > i2;
    }
  }

  private static class MinIntComp implements IntCompare {
    @Override
    public boolean test(int i1, int i2) {
      return i1 < i2;
    }
  }

  private static class MaxFloatComp implements FloatCompare {
    @Override
    public boolean test(float i1, float i2) {
      return i1 > i2;
    }
  }

  private static class MinFloatComp implements FloatCompare {
    @Override
    public boolean test(float i1, float i2) {
      return i1 < i2;
    }
  }

  private static class MaxLongComp implements LongCompare {
    @Override
    public boolean test(long i1, long i2) {
      return i1 > i2;
    }
  }

  private static class MinLongComp implements LongCompare {
    @Override
    public boolean test(long i1, long i2) {
      return i1 < i2;
    }
  }

  /** returns the number of arguments that are non null */
  private static final int numNotNull(final Object... args) {
    int r = 0;
    for (final Object o : args) {
      if (null != o) {
        r++;
      }
    }
    return r;
  }

  /**
   * Helper method for rewriting the Sort associated with a SortSpec. Handles the special case
   * default of relevancy sort (ie: a SortSpec w/null Sort object)
   */
  public static Sort rewriteSort(SortSpec sortSpec, IndexSearcher searcher) throws IOException {
    assert null != sortSpec : "SortSpec must not be null";
    assert null != searcher : "Searcher must not be null";
    Sort orig = sortSpec.getSort();
    if (null == orig) {
      orig = Sort.RELEVANCE;
    }
    return orig.rewrite(searcher);
  }
}
