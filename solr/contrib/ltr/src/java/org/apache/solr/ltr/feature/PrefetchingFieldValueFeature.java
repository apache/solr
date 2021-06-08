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
package org.apache.solr.ltr.feature;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrDocumentFetcher;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * This feature returns the value of a field in the current document.
 * The field must have stored="true" or docValues="true" properties.
 * Example configuration:
 * <pre>{
  "name":  "rawHits",
  "class": "org.apache.solr.ltr.feature.PrefetchingFieldValueFeature",
  "params": {
      "field": "hits"
  }
}</pre>
 */
public class PrefetchingFieldValueFeature extends FieldValueFeature {
  // used to store all fields from all PrefetchingFieldValueFeatures
  private Set<String> prefetchFields;
  // can be used for debugging to only fetch the field this features uses
  public static final String DISABLE_PREFETCHING_FIELD_VALUE_FEATURE = "disablePrefetchingFieldValueFeature";

  public void setPrefetchFields(Set<String> fields) {
    prefetchFields = fields;
  }

  @VisibleForTesting
  public Set<String> getPrefetchFields(){
    return prefetchFields;
  }

  public PrefetchingFieldValueFeature(String name, Map<String,Object> params) {
    super(name, params);
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
      SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi)
          throws IOException {
    return new PrefetchingFieldValueFeatureWeight(searcher, request, originalQuery, efi);
  }

  public class PrefetchingFieldValueFeatureWeight extends FieldValueFeatureWeight {
    private final SolrDocumentFetcher docFetcher;
    private final Boolean disablePrefetching;

    public PrefetchingFieldValueFeatureWeight(IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) {
      super(searcher, request, originalQuery, efi);

      disablePrefetching = request.getParams().getBool(DISABLE_PREFETCHING_FIELD_VALUE_FEATURE, false);
      // get the searcher directly from the request to be sure that we have a SolrIndexSearcher
      this.docFetcher = request.getSearcher().getDocFetcher();
    }

    /**
     * Return a FeatureScorer that works with stored fields and makes use of the cache if the configured field is stored
     * and has no docValues.
     * Otherwise, delegate the work to the FieldValueFeature.
     *
     * @param context the segment this FeatureScorer is working with
     * @return FeatureScorer for the current segment and field
     * @throws IOException as defined by abstract class Feature
     */
    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      if (schemaField != null && !schemaField.stored() && schemaField.hasDocValues()) {
        return super.scorer(context);
      }
      return new PrefetchingFieldValueFeatureScorer(this, context,
          DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
    }

    /**
     * A FeatureScorer that reads the stored value for a field
     * docFetcher does not request a single field but all the prefetchFields to improve performance through caching
     */
    public class PrefetchingFieldValueFeatureScorer extends FieldValueFeatureScorer {

      public PrefetchingFieldValueFeatureScorer(FeatureWeight weight,
          LeafReaderContext context, DocIdSetIterator itr) {
        super(weight, context, itr);
      }

      @Override
      public float score() throws IOException {
        try {
          final Document document;
          if(disablePrefetching) {
            document = docFetcher.doc(context.docBase + itr.docID(), getFieldAsSet());
          } else {
            document = docFetcher.doc(context.docBase + itr.docID(), prefetchFields);
          }
          return super.parseStoredFieldValue(document.getField(getField()));
        } catch (final IOException e) {
          final String prefetchedFields = disablePrefetching ? getField() : StrUtils.join(prefetchFields, ',');
          throw new FeatureException(
              e.toString() + ": " +
                  "Unable to extract feature for " + name +
                  " , tried to prefetch fields " + prefetchedFields +
                  ".\nSet " + DISABLE_PREFETCHING_FIELD_VALUE_FEATURE + " to true to fetch fields individually (only for debugging purposes).", e);
        }
      }
    }
  }
}
