package com.flipkart.solr.ltr.transformers;

import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import com.flipkart.solr.ltr.utils.SolrQueryRequestContextUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.transform.DocTransformer;

public class ScoreMetaTransformer<ScoreMeta> extends DocTransformer {
  final private String name;
  final private SolrQueryRequest req;
  final private ScoreMetaSerializer<ScoreMeta> scoreMetaSerializer;
  private ScoreMetaHolder<ScoreMeta> scoreMetaHolder;

  ScoreMetaTransformer(String name, SolrQueryRequest req, ScoreMetaSerializer<ScoreMeta> scoreMetaSerializer) {
    this.name = name;
    this.req = req;
    this.scoreMetaSerializer = scoreMetaSerializer;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setContext(ResultContext context) {
    super.setContext(context);
    this.scoreMetaHolder = SolrQueryRequestContextUtils.getScoringQuery(req).getScoreMetaHolder();
  }

  @Override
  public void transform(SolrDocument doc, int docId, float score) {
    implTransform(doc, docId);
  }

  @Override
  public void transform(SolrDocument doc, int docId) {
    implTransform(doc, docId);
  }

  private void implTransform(SolrDocument doc, int docId) {
    ScoreMeta scoreMeta = scoreMetaHolder.getScoreMeta(docId);
    doc.addField(name, scoreMetaSerializer.serialize(scoreMeta));
  }
}
