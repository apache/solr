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
package org.apache.solr.ltr.response.transform;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreMode;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ltr.CSVFeatureLogger;
import org.apache.solr.ltr.FeatureLogger;
import org.apache.solr.ltr.LTRRescorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.LTRThreadModule;
import org.apache.solr.ltr.SolrQueryRequestContextUtils;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.interleaving.LTRInterleavingScoringQuery;
import org.apache.solr.ltr.interleaving.OriginalRankingLTRScoringQuery;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.ltr.store.FeatureStore;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.search.DocIterationInfo;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

/**
 * This transformer will take care to generate and append in the response the features declared in
 * the feature store of the current reranking model, or a specified feature store. Ex. <code>
 * fl=id,[features store=myStore efi.user_text="ibm"]</code>
 *
 * <h2>Parameters</h2>
 *
 * <code>store</code> - The feature store to extract features from. If not provided it will default
 * to the features used by your reranking model.<br>
 * <code>efi.*</code> - External feature information variables required by the features you are
 * extracting.<br>
 * <code>format</code> - The format you want the features to be returned in. Supports
 * (dense|sparse). Defaults to dense.<br>
 */
public class LTRFeatureLoggerTransformerFactory extends TransformerFactory {

  // used inside fl to specify the format (dense|sparse) of the extracted features
  private static final String FV_FORMAT = "format";

  // used inside fl to specify the feature store to use for the feature extraction
  private static final String FV_STORE = "store";

  // used inside fl to specify to log (all|model only) features
  private static final String FV_LOG_ALL = "logAll";

  private static final String DEFAULT_LOGGING_MODEL_NAME = "logging-model";

  private static final boolean DEFAULT_NO_RERANKING_LOGGING_ALL = true;

  private String fvCacheName;
  private String loggingModelName = DEFAULT_LOGGING_MODEL_NAME;
  private String defaultStore;
  private FeatureLogger.FeatureFormat defaultFormat = FeatureLogger.FeatureFormat.DENSE;
  private char csvKeyValueDelimiter = CSVFeatureLogger.DEFAULT_KEY_VALUE_SEPARATOR;
  private char csvFeatureSeparator = CSVFeatureLogger.DEFAULT_FEATURE_SEPARATOR;

  private LTRThreadModule threadManager = null;

  public void setFvCacheName(String fvCacheName) {
    this.fvCacheName = fvCacheName;
  }

  public void setLoggingModelName(String loggingModelName) {
    this.loggingModelName = loggingModelName;
  }

  public void setDefaultStore(String defaultStore) {
    this.defaultStore = defaultStore;
  }

  public void setDefaultFormat(String defaultFormat) {
    this.defaultFormat =
        FeatureLogger.FeatureFormat.valueOf(defaultFormat.toUpperCase(Locale.ROOT));
  }

  public void setCsvKeyValueDelimiter(String csvKeyValueDelimiter) {
    if (csvKeyValueDelimiter.length() != 1) {
      throw new IllegalArgumentException("csvKeyValueDelimiter must be exactly 1 character");
    }
    this.csvKeyValueDelimiter = csvKeyValueDelimiter.charAt(0);
  }

  public void setCsvFeatureSeparator(String csvFeatureSeparator) {
    if (csvFeatureSeparator.length() != 1) {
      throw new IllegalArgumentException("csvFeatureSeparator must be exactly 1 character");
    }
    this.csvFeatureSeparator = csvFeatureSeparator.charAt(0);
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    threadManager = LTRThreadModule.getInstance(args);
    SolrPluginUtils.invokeSetters(this, args);
  }

  @Override
  public DocTransformer create(String name, SolrParams localparams, SolrQueryRequest req) {

    // Hint to enable feature vector cache since we are requesting features
    SolrQueryRequestContextUtils.enableFeatureLogging(req);

    // Communicate which feature store we are requesting features for
    final String fvStoreName = localparams.get(FV_STORE);
    SolrQueryRequestContextUtils.setFvStoreName(
        req, (fvStoreName == null ? defaultStore : fvStoreName));

    Boolean logAll = localparams.getBool(FV_LOG_ALL);

    // Create and supply the feature logger to be used
    SolrQueryRequestContextUtils.setFeatureLogger(
        req, createFeatureLogger(localparams.get(FV_FORMAT), logAll));

    return new FeatureTransformer(
        name, localparams, req, (fvStoreName != null) /* hasExplicitFeatureStore */);
  }

  /**
   * returns a FeatureLogger that logs the features 'featureFormat' param: 'dense' will write
   * features in dense format, 'sparse' will write the features in sparse format, null or empty will
   * default to 'sparse'
   *
   * @return a feature logger for the format specified.
   */
  private FeatureLogger createFeatureLogger(String formatStr, Boolean logAll) {
    final FeatureLogger.FeatureFormat format;
    if (formatStr != null) {
      format = FeatureLogger.FeatureFormat.valueOf(formatStr.toUpperCase(Locale.ROOT));
    } else {
      format = this.defaultFormat;
    }
    if (fvCacheName == null) {
      throw new IllegalArgumentException("a fvCacheName must be configured");
    }
    return new CSVFeatureLogger(
        fvCacheName, format, logAll, csvKeyValueDelimiter, csvFeatureSeparator);
  }

  class FeatureTransformer extends DocTransformer {

    private final String name;
    private final SolrParams localparams;
    private final SolrQueryRequest req;
    private final boolean hasExplicitFeatureStore;

    private List<LeafReaderContext> leafContexts;
    private SolrIndexSearcher searcher;

    /**
     * rerankingQueries, modelWeights have:
     *
     * <p>length=1 - [Classic LTR] When reranking with a single model
     *
     * <p>length=2 - [Interleaving] When reranking with interleaving (two ranking models are
     * involved)
     */
    private LTRScoringQuery[] rerankingQueriesFromContext;

    private LTRScoringQuery[] rerankingQueries;
    private LTRScoringQuery.ModelWeight[] modelWeights;
    private FeatureLogger featureLogger;
    private boolean docsWereReranked;
    private boolean docsHaveScores;

    /**
     * @param name Name of the field to be added in a document representing the feature vectors
     */
    public FeatureTransformer(
        String name,
        SolrParams localparams,
        SolrQueryRequest req,
        boolean hasExplicitFeatureStore) {
      this.name = name;
      this.localparams = localparams;
      this.req = req;
      this.hasExplicitFeatureStore = hasExplicitFeatureStore;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setContext(ResultContext context) {
      super.setContext(context);
      if (context == null) {
        return;
      }
      if (context.getRequest() == null) {
        return;
      }

      searcher = context.getSearcher();
      if (searcher == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "searcher is null");
      }
      leafContexts = searcher.getTopReaderContext().leaves();
      if (threadManager != null) {
        threadManager.setExecutor(
            context.getRequest().getCoreContainer().getUpdateShardHandler().getUpdateExecutor());
      }

      rerankingQueriesFromContext = SolrQueryRequestContextUtils.getScoringQueries(req);
      docsWereReranked =
          (rerankingQueriesFromContext != null && rerankingQueriesFromContext.length != 0);
      docsHaveScores = context.wantsScores();
      String transformerFeatureStore = SolrQueryRequestContextUtils.getFvStoreName(req);
      FeatureLogger featureLogger = SolrQueryRequestContextUtils.getFeatureLogger(req);

      Map<String, String[]> transformerExternalFeatureInfo =
          LTRQParserPlugin.extractEFIParams(localparams);
      List<Feature> modelFeatures = null;

      if (docsWereReranked) {
        LTRScoringModel scoringModel = rerankingQueriesFromContext[0].getScoringModel();
        modelFeatures = scoringModel.getFeatures();
      } else {
        if (featureLogger.isLoggingAll() == null) {
          featureLogger.setLogAll(DEFAULT_NO_RERANKING_LOGGING_ALL);
        }
        if (!featureLogger.isLoggingAll()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "you can only log all features from the store '"
                  + transformerFeatureStore
                  + "' passed in input in the logger");
        }
      }

      final LoggingModel loggingModel =
          createLoggingModel(
              transformerFeatureStore,
              featureLogger.isLoggingAll(),
              modelFeatures,
              docsWereReranked);
      setupRerankingQueriesForLogging(
          transformerFeatureStore, transformerExternalFeatureInfo, loggingModel);
      setupRerankingWeightsForLogging(context, featureLogger);
    }

    /**
     * The loggingModel is an empty model that is just used to extract the features and log them
     *
     * @param transformerFeatureStore the explicit transformer feature store
     */
    private LoggingModel createLoggingModel(
        String transformerFeatureStore,
        boolean logAll,
        List<Feature> modelFeatures,
        boolean docsWereReranked) {
      final ManagedFeatureStore featureStores =
          ManagedFeatureStore.getManagedFeatureStore(req.getCore());

      // check for non-existent feature store name
      if (transformerFeatureStore != null) {
        if (!featureStores.getStores().containsKey(transformerFeatureStore)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Missing feature store: " + transformerFeatureStore);
        }
      }

      final FeatureStore store = featureStores.getFeatureStore(transformerFeatureStore);

      // check for empty feature store only if there is no reranking query, otherwise the model
      // store will be used later for feature extraction
      if (!docsWereReranked) {
        if (store.getFeatures().isEmpty()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Empty feature store. If no ranking query (rq) is passed and DEFAULT feature store is empty, an existing feature store name must be passed for feature extraction.");
        }
      }

      // if transformerFeatureStore was null before this gets actual name
      transformerFeatureStore = store.getName();

      return new LoggingModel(
          loggingModelName,
          transformerFeatureStore,
          (logAll ? store.getFeatures() : modelFeatures));
    }

    /**
     * When preparing the reranking queries for logging features various scenarios apply:
     *
     * <p>No Reranking
     *
     * <p>There is the need of a logger model from the default feature store or the explicit feature
     * store passed to extract the feature vector
     *
     * <p>Re Ranking
     *
     * <p>1) If no explicit feature store is passed, the models for each reranking query can be
     * safely re-used the feature vector can be fetched from the feature vector cache.
     *
     * <p>2) If an explicit feature store is passed, and no reranking query uses a model with that
     * feature store, There is the need of a logger model to extract the feature vector
     *
     * <p>3) If an explicit feature store is passed, and there is a reranking query that uses a
     * model with that feature store, the model can be re-used and there is no need for a logging
     * model
     *
     * @param transformerFeatureStore explicit feature store for the transformer
     * @param transformerExternalFeatureInfo explicit efi for the transformer
     */
    private void setupRerankingQueriesForLogging(
        String transformerFeatureStore,
        Map<String, String[]> transformerExternalFeatureInfo,
        LoggingModel loggingModel) {
      if (!docsWereReranked) { // no reranking query
        LTRScoringQuery loggingQuery =
            new LTRScoringQuery(loggingModel, transformerExternalFeatureInfo, threadManager);
        rerankingQueries = new LTRScoringQuery[] {loggingQuery};
      } else {
        rerankingQueries = new LTRScoringQuery[rerankingQueriesFromContext.length];
        System.arraycopy(
            rerankingQueriesFromContext,
            0,
            rerankingQueries,
            0,
            rerankingQueriesFromContext.length);

        if (transformerFeatureStore != null) { // explicit feature store for the transformer
          LTRScoringModel matchingRerankingModel = loggingModel;
          for (LTRScoringQuery rerankingQuery : rerankingQueries) {
            if (!(rerankingQuery instanceof OriginalRankingLTRScoringQuery)
                && transformerFeatureStore.equals(
                    rerankingQuery.getScoringModel().getFeatureStoreName())) {
              matchingRerankingModel = rerankingQuery.getScoringModel();
            }
          }

          for (int i = 0; i < rerankingQueries.length; i++) {
            rerankingQueries[i] =
                new LTRScoringQuery(
                    matchingRerankingModel,
                    (!transformerExternalFeatureInfo.isEmpty()
                        ? transformerExternalFeatureInfo
                        : rerankingQueries[i].getExternalFeatureInfo()),
                    threadManager);
          }
        }
      }
    }

    private void setupRerankingWeightsForLogging(ResultContext context, FeatureLogger logger) {
      modelWeights = new LTRScoringQuery.ModelWeight[rerankingQueries.length];
      for (int i = 0; i < rerankingQueries.length; i++) {
        if (rerankingQueries[i].getOriginalQuery() == null) {
          rerankingQueries[i].setOriginalQuery(context.getQuery());
        }
        rerankingQueries[i].setRequest(req);
        if (!(rerankingQueries[i] instanceof OriginalRankingLTRScoringQuery)
            || hasExplicitFeatureStore) {
          if (rerankingQueries[i].getFeatureLogger() == null) {
            rerankingQueries[i].setFeatureLogger(logger);
          }
          featureLogger = rerankingQueries[i].getFeatureLogger();
          try {
            modelWeights[i] = rerankingQueries[i].createWeight(searcher, ScoreMode.COMPLETE, 1f);
          } catch (final IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
          }
          if (modelWeights[i] == null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "error logging the features, model weight is null");
          }
        }
      }
    }

    @Override
    public void transform(SolrDocument doc, int docid, DocIterationInfo docInfo)
        throws IOException {
      implTransform(doc, docid, docInfo);
    }

    private void implTransform(SolrDocument doc, int docid, DocIterationInfo docInfo)
        throws IOException {
      LTRScoringQuery rerankingQuery = rerankingQueries[0];
      LTRScoringQuery.ModelWeight rerankingModelWeight = modelWeights[0];
      for (int i = 1; i < rerankingQueries.length; i++) {
        if (((LTRInterleavingScoringQuery) rerankingQueriesFromContext[i])
            .getPickedInterleavingDocIds()
            .contains(docid)) {
          rerankingQuery = rerankingQueries[i];
          rerankingModelWeight = modelWeights[i];
        }
      }
      if (!(rerankingQuery instanceof OriginalRankingLTRScoringQuery) || hasExplicitFeatureStore) {
        Object featureVector = featureLogger.getFeatureVector(docid, rerankingQuery, searcher);
        if (featureVector == null) { // FV for this document was not in the cache
          featureVector =
              featureLogger.makeFeatureVector(
                  LTRRescorer.extractFeaturesInfo(
                      rerankingModelWeight,
                      docid,
                      (!docsWereReranked && docsHaveScores) ? docInfo.score() : null,
                      leafContexts));
        }
        doc.addField(name, featureVector);
      }
    }
  }

  private static class LoggingModel extends LTRScoringModel {

    public LoggingModel(String name, String featureStoreName, List<Feature> allFeatures) {
      this(
          name,
          Collections.emptyList(),
          Collections.emptyList(),
          featureStoreName,
          allFeatures,
          Collections.emptyMap());
    }

    protected LoggingModel(
        String name,
        List<Feature> features,
        List<Normalizer> norms,
        String featureStoreName,
        List<Feature> allFeatures,
        Map<String, Object> params) {
      super(name, features, norms, featureStoreName, allFeatures, params);
    }

    @Override
    public float score(float[] modelFeatureValuesNormalized) {
      return 0;
    }

    @Override
    public Explanation explain(
        LeafReaderContext context,
        int doc,
        float finalScore,
        List<Explanation> featureExplanations) {
      return Explanation.match(
          finalScore, toString() + " logging model, used only for logging the features");
    }
  }
}
