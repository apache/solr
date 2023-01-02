package com.flipkart.solr.ltr.transformers;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;

public abstract class ScoreMetaTransformerFactory<ScoreMeta> extends TransformerFactory {
  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    return new ScoreMetaTransformer<>(field, req, provideScoreMetaSerializer());
  }

  protected abstract ScoreMetaSerializer<ScoreMeta> provideScoreMetaSerializer();
}
