package com.flipkart.neo.solr.ltr.schema.parser;

import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import org.apache.solr.common.SolrInputDocument;

public class InputDocumentParser extends DocumentParser<SolrInputDocument> {

  public InputDocumentParser(BannerScorer bannerScorer, ObjectMapper mapper) {
    super(bannerScorer, mapper);
  }

  @Override
  protected Integer getIntegerFieldValue(SolrInputDocument document, String fieldName) {
    return (Integer) document.getFieldValue(fieldName);
  }

  @Override
  protected Long getLongFieldValue(SolrInputDocument document, String fieldName) {
    return (Long) document.getFieldValue(fieldName);
  }

  @Override
  protected Double getDoubleFieldValue(SolrInputDocument document, String fieldName) {
    return (Double) document.getFieldValue(fieldName);
  }

  @Override
  protected String getStringFieldValue(SolrInputDocument document, String fieldName) {
    return (String) document.getFieldValue(fieldName);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Collection<String> getStringFieldValues(SolrInputDocument document, String fieldName) {
    return (Collection) document.getFieldValues(fieldName);
  }
}
