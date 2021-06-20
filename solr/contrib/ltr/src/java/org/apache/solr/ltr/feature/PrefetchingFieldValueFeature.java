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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This feature returns the value of a field in the current document.
 * The field must have stored="true" or docValues="true" properties, otherwise this feature returns a default value.
 * <p>
 * This feature will not only fetch the field that itself uses, but also all fields that are used by the other
 * PrefetchingFieldValueFeatures in the feature-store.
 * This results in a performance benefit compared to the {@link FieldValueFeature} for stored fields that
 * do not have docValues.
 * </p>
 * Example configuration:
 * <pre>{
  "name":  "rawHits",
  "class": "org.apache.solr.ltr.feature.PrefetchingFieldValueFeature",
  "params": {
      "field": "hits"
  }
}</pre>
 * NOTE: To best utilize prefetching, use separate feature-stores for different models. This avoids unnecessary
 * fetching of fields.
 */
public class PrefetchingFieldValueFeature extends FieldValueFeature {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // used to store all fields from all PrefetchingFieldValueFeatures
  private SortedSet<String> prefetchFields;

  public void setPrefetchFields(SortedSet<String> fields) {
    prefetchFields = fields;
  }

  // needed for loading from storage
  public void setPrefetchFields(Collection<String> fields) {
    prefetchFields = new TreeSet<>(fields);
  }

  @VisibleForTesting
  public SortedSet<String> getPrefetchFields(){
    return prefetchFields;
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = defaultParamsToMap();
    params.put("field", getField());
    params.put("prefetchFields", prefetchFields == null ? Collections.emptySet() : prefetchFields); // prevent NPE
    return params;
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

    public PrefetchingFieldValueFeatureWeight(IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) {
      super(searcher, request, originalQuery, efi);

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
        final Document document = fetchDocument();
        return super.parseStoredFieldValue(document.getField(getField()));
      }

      // not private to enable possible subclasses
      protected Document fetchDocument() throws FeatureException {
        try {
          return docFetcher.doc(context.docBase + itr.docID(), prefetchFields);
        } catch (final IOException exAllFields) {
          try {
            // this should only happen in rare cases when we fail to read from the index
            // try to avoid the fallback to the default value by fetching only the field for this feature
            // log an error because this breaks the prefetch-functionality and should be noticed
            log.error("Unable to fetch document with prefetchFields " + StrUtils.join(prefetchFields, ',') +
                "! Will try to only fetch field " + getField() + " for feature " + name +
                ". Cause for failure: " + exAllFields.getMessage());
            final Document document = docFetcher.doc(context.docBase + itr.docID(), getFieldAsSet());
            log.info("Fallback to fetch single field " + getField() + " for feature " + name + " was successful.");
            return document;
          } catch (final IOException exSingleField) {
            // even the fallback to single field was unsuccessful
            throw new FeatureException("Unable to extract feature for " + name +
                    " , after unsuccessful fallback to only fetch field " + getField(), exSingleField);
          }
        }
      }
    }
  }
}
