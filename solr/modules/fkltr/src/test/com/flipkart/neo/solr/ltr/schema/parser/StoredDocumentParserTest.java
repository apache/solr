package com.flipkart.neo.solr.ltr.schema.parser;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.junit.Test;

public class StoredDocumentParserTest extends DocumentParserTest<Document> {
  private ObjectMapper objectMapper;

  @Override
  protected DocumentParser<Document> getConcreteParser(BannerScorer bannerScorer, ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    return new StoredDocumentParser(bannerScorer, objectMapper);
  }

  @Override
  protected Document getDocument() throws JsonProcessingException {
    Document document = new Document();
    document.add(new StringField(SchemaFieldNames.BANNER_ID, BANNER_ID, Field.Store.NO));
    document.add(new LongPoint(SchemaFieldNames.VERSION, VERSION));
    document.add(new StringField(SchemaFieldNames.CREATIVE_ID, CREATIVE_ID, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.BANNER_GROUP_ID, BANNER_GROUP_ID, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.LOCALE, LOCALE, Field.Store.NO));
    for (String themeNamePlusTemplateId: THEME_NAME_PLUS_TEMPLATE_ID_STRINGS) {
      document.add(new StringField(SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS, themeNamePlusTemplateId, Field.Store.NO));
    }
    document.add(new StringField(SchemaFieldNames.WIDGET_TYPE, WIDGET_TYPE, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.TEAM_TYPE, TEAM_TYPE, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.RELEVANT_STORES, objectMapper.writeValueAsString(RELEVANT_STORES), Field.Store.NO));
    document.add(new DoublePoint(SchemaFieldNames.ASP, ASP));
    document.add(new DoublePoint(SchemaFieldNames.BANNER_GROUP_PRICE_VALUE, BANNER_GROUP_PRICE_VALUE));
    document.add(new StringField(SchemaFieldNames.BRANDS, objectMapper.writeValueAsString(BRANDS), Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.RELEVANT_STORE_SCORES, RELEVANT_STORE_SCORES, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.SIMILAR_STORE_SCORES, SIMILAR_STORE_SCORES, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.CROSS_STORE_SCORES, CROSS_STORE_SCORES, Field.Store.NO));
    document.add(new StringField(SchemaFieldNames.EXCLUDED_STORES, objectMapper.writeValueAsString(EXCLUDED_STORES), Field.Store.NO));
    return document;
  }

  @Test
  public void testParsing() throws IOException {
    super.testParsing();
  }
}
