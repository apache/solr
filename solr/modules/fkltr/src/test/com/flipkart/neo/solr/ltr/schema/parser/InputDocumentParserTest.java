package com.flipkart.neo.solr.ltr.schema.parser;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class InputDocumentParserTest extends DocumentParserTest<SolrInputDocument> {
  private ObjectMapper objectMapper;

  @Override
  protected DocumentParser<SolrInputDocument> getConcreteParser(BannerScorer bannerScorer, ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    return new InputDocumentParser(bannerScorer, objectMapper);
  }

  @Override
  protected SolrInputDocument getDocument() throws JsonProcessingException {
    SolrInputDocument document = new SolrInputDocument();
    document.addField(SchemaFieldNames.BANNER_ID, BANNER_ID);
    document.addField(SchemaFieldNames.VERSION, VERSION);
    document.addField(SchemaFieldNames.CREATIVE_ID, CREATIVE_ID);
    document.addField(SchemaFieldNames.BANNER_GROUP_ID, BANNER_GROUP_ID);
    document.addField(SchemaFieldNames.LOCALE, LOCALE);
    for (String themeNamePlusTemplateId: THEME_NAME_PLUS_TEMPLATE_ID_STRINGS) {
      document.addField(SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS, themeNamePlusTemplateId);
    }
    document.addField(SchemaFieldNames.WIDGET_TYPE, WIDGET_TYPE);
    document.addField(SchemaFieldNames.TEAM_TYPE, TEAM_TYPE);
    document.addField(SchemaFieldNames.RELEVANT_STORES, objectMapper.writeValueAsString(RELEVANT_STORES));
    document.addField(SchemaFieldNames.ASP, ASP);
    document.addField(SchemaFieldNames.BANNER_GROUP_PRICE_VALUE, BANNER_GROUP_PRICE_VALUE);
    document.addField(SchemaFieldNames.BRANDS, objectMapper.writeValueAsString(BRANDS));
    document.addField(SchemaFieldNames.RELEVANT_STORE_SCORES, RELEVANT_STORE_SCORES);
    document.addField(SchemaFieldNames.SIMILAR_STORE_SCORES, SIMILAR_STORE_SCORES);
    document.addField(SchemaFieldNames.CROSS_STORE_SCORES, CROSS_STORE_SCORES);
    document.addField(SchemaFieldNames.EXCLUDED_STORES, objectMapper.writeValueAsString(EXCLUDED_STORES));
    return document;
  }

  @Test
  public void testParsing() throws IOException {
    super.testParsing();
  }
}
