package com.flipkart.neo.solr.ltr.schema.parser;

import java.util.Arrays;
import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

public class StoredDocumentParser extends DocumentParser<Document> {

  public StoredDocumentParser(BannerScorer bannerScorer, ObjectMapper mapper) {
    super(bannerScorer, mapper);
  }

  @Override
  protected Integer getIntegerFieldValue(Document document, String fieldName) {
    IndexableField field = document.getField(fieldName);
    if (field != null) {
      return (Integer) field.numericValue();
    } else {
      return null;
    }
  }

  @Override
  protected Long getLongFieldValue(Document document, String fieldName) {
    IndexableField field = document.getField(fieldName);
    if (field != null) {
      return (Long) field.numericValue();
    } else {
      return null;
    }
  }

  @Override
  protected Double getDoubleFieldValue(Document document, String fieldName) {
    IndexableField field = document.getField(fieldName);
    if (field != null) {
      return field.numericValue().doubleValue();
    } else {
      return null;
    }
  }

  @Override
  protected String getStringFieldValue(Document document, String fieldName) {
    return document.get(fieldName);
  }

  @Override
  protected Collection<String> getStringFieldValues(Document document, String fieldName) {
    String[] values = document.getValues(fieldName);

    if (values == null || values.length == 0)
      return null;
    else return Arrays.asList(values);
  }
}
