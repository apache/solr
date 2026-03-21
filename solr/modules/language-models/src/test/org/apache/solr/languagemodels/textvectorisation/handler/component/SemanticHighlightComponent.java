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
package org.apache.solr.languagemodels.textvectorisation.handler.component;

import java.util.Comparator;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.highlight.UnifiedSolrHighlighter;
import org.apache.solr.languagemodels.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.languagemodels.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.util.plugin.SolrCoreAware;

public class SemanticHighlightComponent extends HighlightComponent
    implements SolrCoreAware, ManagedResourceObserver {

  private ManagedTextToVectorModelStore modelStore = null;

  @Override
  public void inform(SolrCore core) {
    super.inform(core);
    ManagedTextToVectorModelStore.registerManagedTextToVectorModelStore(
        core.getResourceLoader(), this);
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
      throws SolrException {
    if (res instanceof ManagedTextToVectorModelStore) {
      modelStore = (ManagedTextToVectorModelStore) res;
    }
    if (modelStore != null) {
      modelStore.loadStoredModels();
    }
  }

  @Override
  public SolrHighlighter getHighlighter(SolrParams params) {
    if ("unified_with_semantic".equals(params.get(HighlightParams.METHOD))) {

      return new UnifiedSolrHighlighter() {
        @Override
        protected UnifiedHighlighter getHighlighter(SolrQueryRequest req) {

          final ManagedTextToVectorModelStore modelStore =
              ManagedTextToVectorModelStore.getManagedModelStore(req.getCore());

          final String modelName = req.getParams().get("hl.unified_with_semantic.model");
          final SolrTextToVectorModel textToVector = modelStore.getModel(modelName);
          if (textToVector == null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "The model requested '" + modelName + "' can't be found.");
          }

          final float[] queryVector;
          final String queryVectorString = req.getParams().get("hl.unified_with_semantic.vector");
          if (queryVectorString != null) {
            final String[] queryVectorArray = queryVectorString.split(",");
            queryVector = new float[queryVectorArray.length];
            for (int i = 0; i < queryVectorArray.length; i++) {
              queryVector[i] = Float.parseFloat(queryVectorArray[i]);
            }
          } else {
            queryVector = null;
          }

          return new SolrExtendedUnifiedHighlighter(req) {

            private static String textFromPassage(Passage passage) {
              final StringBuilder sb = new StringBuilder();
              String delimiter = "";
              for (BytesRef term : passage.getMatchTerms()) {
                if (term != null) {
                  sb.append(delimiter).append(term.utf8ToString());
                  delimiter = " ";
                }
              }
              return sb.toString();
            }

            private static double euclideanDistance(float[] a, float[] b) {
              double sumSquared = 0.0;
              for (int i = 0; i < a.length; i++) {
                double diff = a[i] - b[i];
                sumSquared += diff * diff;
              }
              return Math.sqrt(sumSquared);
            }

            @Override
            protected Comparator<Passage> getPassageSortComparator(String field) {
              return new Comparator<Passage>() {
                @Override
                public int compare(Passage a, Passage b) {
                  String aText = textFromPassage(a);
                  String bText = textFromPassage(b);
                  if (queryVector == null) {
                    return aText.compareTo(bText);
                  } else {
                    float[] aVector = textToVector.vectorise(aText);
                    float[] bVector = textToVector.vectorise(bText);
                    double aDistance = euclideanDistance(aVector, queryVector);
                    double bDistance = euclideanDistance(bVector, queryVector);
                    return Double.compare(aDistance, bDistance);
                  }
                }
              };
            }
          };
        }
      };
    } else {
      return super.getHighlighter(params);
    }
  }
}
