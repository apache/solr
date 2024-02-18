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
package org.apache.solr.parser;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.queryparser.charstream.CharStream;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class SolrQueryParserBaseTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setUpClass() {
    assumeWorkingMockito();
  }

  private static final String DEFAULT_FIELD_NAME = "TestDefaultFieldname";

  private static class MockSolrQueryParser extends SolrQueryParserBase {
    @Override
    public void ReInit(CharStream stream) {}

    @Override
    public Query TopLevelQuery(String field) {
      return null;
    }
  }

  private final QParser qParser = Mockito.mock(QParser.class);
  private final QParser subQParser = Mockito.mock(QParser.class);
  private final SolrQueryRequest solrQueryRequest = Mockito.mock(SolrQueryRequest.class);
  private final Query query = Mockito.mock(Query.class);
  private final IndexSchema indexSchema = Mockito.mock(IndexSchema.class);

  private MockSolrQueryParser solrQueryParser;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    solrQueryParser = new MockSolrQueryParser();
  }

  private void initQParser() {
    doReturn(indexSchema).when(solrQueryRequest).getSchema();
    doReturn(solrQueryRequest).when(qParser).getReq();
  }

  @Test
  public void testInitHappyCases() {
    initQParser();
    solrQueryParser.init(null, qParser);
    solrQueryParser.init(DEFAULT_FIELD_NAME, qParser);
  }

  @Test(expected = SolrException.class)
  public void testInitBadDefaultField() {
    solrQueryParser.init("", qParser);
  }

  @Test(expected = SolrException.class)
  public void testInitNullQParser() {
    solrQueryParser.init(DEFAULT_FIELD_NAME, null);
  }

  @Test(expected = SolrException.class)
  public void testInitNullQParserReq() {
    solrQueryParser.init(DEFAULT_FIELD_NAME, qParser);
  }

  @Test(expected = SolrException.class)
  public void testInitNullQParserReqSchema() {
    doReturn(solrQueryRequest).when(qParser).getReq();
    solrQueryParser.init(DEFAULT_FIELD_NAME, qParser);
  }

  @Test
  public void testGetField() {
    initQParser();
    solrQueryParser.init(DEFAULT_FIELD_NAME, qParser);
    assertEquals(DEFAULT_FIELD_NAME, solrQueryParser.getField(null));
    assertEquals(DEFAULT_FIELD_NAME, solrQueryParser.getField(""));
    final String nonNullFieldName = "testFieldName";
    assertEquals(nonNullFieldName, solrQueryParser.getField(nonNullFieldName));
  }

  @Test
  public void testGetMagicFieldQuery() throws Exception {
    String magicField = "_val_";
    String magicFieldSubParser = SolrQueryParserBase.MagicFieldName.get(magicField).subParser;
    initQParser();
    solrQueryParser.init(DEFAULT_FIELD_NAME, qParser);
    solrQueryParser.setAllowSubQueryParsing(true);
    doReturn(query).when(subQParser).getQuery();
    doReturn(subQParser).when(qParser).subQuery(anyString(), eq(magicFieldSubParser));

    String queryText = "queryText";
    List<String> queryTerms = Arrays.asList("query", "terms");
    boolean quoted = true; // value doesn't matter for this test
    boolean raw = true; // value doesn't matter for this test
    assertEquals(query, solrQueryParser.getFieldQuery(magicField, queryText, quoted, raw));
    assertEquals(query, solrQueryParser.getFieldQuery(magicField, queryTerms, raw));
  }
}
