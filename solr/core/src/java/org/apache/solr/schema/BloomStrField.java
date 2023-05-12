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
package org.apache.solr.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;

/**
 * A specialized StrField variant that facilitates configuration of a ngram subfield (populated at
 * segment flush by a custom PostingsFormat) that can be used to pre-filter terms that must be
 * evaluated for substring/wildcard/regex search.
 */
public final class BloomStrField extends StrField implements SchemaAware {

  public static final String DEFAULT_BLOOM_ANALYZER_ID = "TRIGRAM";

  public static String constructName(PostingsFormat delegate, String bloomAnalyzerId) {
    return "Bloom" + delegate.getName() + "x" + bloomAnalyzerId;
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  /**
   * Base suffix must start with `0` to ensure that FieldsProducer visits the subfield immediately
   * after the parent field -- i.e., the fieldnames must sort lexicographically adjacent.
   */
  public static final String BLOOM_FIELD_BASE_SUFFIX = "0NgramS"; // single-valued

  public static final String BLOOM_FIELD_BASE_SUFFIX_MULTI = "0NgramM"; // multiValued

  private IndexSchema schema;
  private FieldType bloomFieldType;
  private static final Analyzer KEYWORD_ANALYZER =
      new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tk = new KeywordTokenizer();
          return new TokenStreamComponents(tk, tk);
        }
      };

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    this.schema = schema;
    String bloomAnalyzerId = args.remove("bloomAnalyzerId");
    String postingsFormat = args.get("postingsFormat");
    PostingsFormat pf;
    if (postingsFormat != null) {
      pf = PostingsFormat.forName(postingsFormat);
    } else {
      // start with the default postingsFormat.
      pf = Codec.getDefault().postingsFormat();
      if (pf instanceof PerFieldPostingsFormat) {
        pf = ((PerFieldPostingsFormat) pf).getPostingsFormatForField("");
      }
    }
    if (pf instanceof BloomAnalyzerSupplier) {
      if (bloomAnalyzerId != null
          && !bloomAnalyzerId.equals(((BloomAnalyzerSupplier) pf).getBloomAnalyzerId())) {
        throw new IllegalArgumentException(
            "specified `bloomAnalyzerId` "
                + bloomAnalyzerId
                + " conflicts with the `bloomAnalyzerId` of the specified `postingsFormat`");
      }
    } else {
      if (bloomAnalyzerId == null) {
        bloomAnalyzerId = DEFAULT_BLOOM_ANALYZER_ID;
      }
      pf = PostingsFormat.forName(constructName(pf, bloomAnalyzerId));
      if (!(pf instanceof BloomAnalyzerSupplier)) {
        throw new IllegalArgumentException(
            "constructed `postingsFormat` does not support ngram bloom filter: " + pf);
      }
      // replace any existing postingsFormat spec with our the constructed postingsFormat
      args.put("postingsFormat", pf.getName());
    }
    bloomFieldType = getFieldType(schema, (PostingsFormat & BloomAnalyzerSupplier) pf);
    super.init(schema, args);
  }

  private <T extends PostingsFormat & BloomAnalyzerSupplier> FieldType getFieldType(
      IndexSchema schema, T pf) {
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");
    props.put("stored", "false");
    props.put("docValues", "false");
    props.put("sortMissingLast", "true");
    props.put("termVectors", "false");
    props.put("omitNorms", "true");
    props.put("omitTermFreqAndPositions", "false");
    props.put("uninvertible", "true");
    props.put("postingsFormat", pf.getName());
    FieldType ret = new TextField();
    ret.setTypeName("ngram_bloom_filter_" + pf.getBloomAnalyzerId());
    ret.setIndexAnalyzer(KEYWORD_ANALYZER);
    ret.setQueryAnalyzer(pf.getBloomAnalyzer());
    // NOTE: we must call `setArgs()` here, as opposed to `init()`, in order to properly
    // set postingsFormat.
    ret.setArgs(schema, props);
    return ret;
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    List<IndexableField> ret = new ArrayList<>(3);
    String bloomFieldName =
        field
            .getName()
            .concat(field.multiValued() ? BLOOM_FIELD_BASE_SUFFIX_MULTI : BLOOM_FIELD_BASE_SUFFIX);

    // reserve a spot in fieldInfos, so that our PostingsFormat sees the subfield
    ret.add(createField(bloomFieldName, "", schema.getField(bloomFieldName)));

    ret.addAll(super.createFields(field, value));
    return ret;
  }

  @Override
  public void inform(IndexSchema schema) {
    registerDynamicSubfield(false);
    registerDynamicSubfield(true);
  }

  private void registerDynamicSubfield(boolean multiValued) {
    String name = "*" + (multiValued ? BLOOM_FIELD_BASE_SUFFIX_MULTI : BLOOM_FIELD_BASE_SUFFIX);
    Map<String, String> props = new HashMap<>();
    props.put("multiValued", Boolean.toString(multiValued));
    int p = SchemaField.calcProps(name, bloomFieldType, props);
    schema.registerDynamicFields(SchemaField.create(name, bloomFieldType, p, null));
  }

  public interface BloomAnalyzerSupplier {
    String getBloomAnalyzerId();

    Analyzer getBloomAnalyzer();
  }

  private final WeakHashMap<IndexReader.CacheKey, Map<String, Analyzer>> bloomAnalyzerCache =
      new WeakHashMap<>();

  /**
   * This method should be used to retrieve a bloom analyzer that is compatible with the analyzer
   * used to build ngram data for the specified source field and the segment corresponding to the
   * specified LeafReader. NOTE: the returned analyzer is determined by the associated segement, and
   * may differ from the bloom analyzer currently specified on the {@link #bloomFieldType} for this
   * {@link BloomStrField} field type.
   */
  public Analyzer getBloomAnalyzer(LeafReader r, String field) {
    IndexReader.CacheHelper cch = r.getCoreCacheHelper();
    IndexReader.CacheKey key = cch == null ? null : cch.getKey();
    Function<? super String, ? extends Analyzer> func =
        (f) -> {
          FieldInfo fi = r.getFieldInfos().fieldInfo(f);
          if (fi == null) {
            return null;
          }
          String postingsFormat = fi.getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY);
          if (postingsFormat == null) {
            return null;
          }
          PostingsFormat pf = PostingsFormat.forName(postingsFormat);
          if (pf instanceof BloomAnalyzerSupplier) {
            return ((BloomAnalyzerSupplier) pf).getBloomAnalyzer();
          }
          return null;
        };
    if (key == null) {
      return func.apply(field);
    } else {
      return bloomAnalyzerCache
          .computeIfAbsent(key, (k) -> new HashMap<>())
          .computeIfAbsent(field, func);
    }
  }
}
