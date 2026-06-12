package org.apache.solr.schema;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;

public class TestNestPathField extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Initializes the core using a standard test schema that contains a NestPathField
    initCore("solrconfig.xml", "schema15.xml");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    clearIndex();
    super.tearDown();
  }

  @Test
  public void testGetFieldQueryRootShortcut() throws Exception {
    try (SolrQueryRequest req = req()) {
      QParser parser = QParser.getParser("*:*", req);
      SchemaField field = req.getSchema().getField("_nest_path_");
      NestPathField type = (NestPathField) field.getType();

      // 1. Verify that a null/omitted value throws a BAD_REQUEST SolrException
      SolrException ex =
          expectThrows(
              SolrException.class,
              () -> {
                type.getFieldQuery(parser, field, null);
              });
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(ex.getMessage().contains("cannot be queried with a null."));

      // 2. Verify that empty string and root slash trigger the root shortcut query
      String[] shortcutInputs = new String[] {"", "/"};
      for (String input : shortcutInputs) {
        Query q = type.getFieldQuery(parser, field, input);

        assertTrue(
            "Query should be a BooleanQuery for input: '" + input + "'", q instanceof BooleanQuery);
        BooleanQuery bq = (BooleanQuery) q;

        assertEquals(
            "Root shortcut BooleanQuery must contain exactly 2 clauses", 2, bq.clauses().size());

        // Assert clause 1: MUST MatchAllDocsQuery
        BooleanClause clause1 = bq.clauses().getFirst();
        assertEquals(BooleanClause.Occur.MUST, clause1.occur());
        assertTrue(clause1.query() instanceof MatchAllDocsQuery);

        // Assert clause 2: MUST_NOT ExistenceQuery
        BooleanClause clause2 = bq.clauses().get(1);
        assertEquals(BooleanClause.Occur.MUST_NOT, clause2.occur());
        assertEquals(type.getExistenceQuery(parser, field), clause2.query());
      }

      // 3. Verify standard paths do not trigger the shortcut and fall back safely
      Query standardQ = type.getFieldQuery(parser, field, "/child");
      assertFalse(
          "A normal path should not return the root shortcut BooleanQuery",
          standardQ instanceof BooleanQuery);

    }
  }
}
