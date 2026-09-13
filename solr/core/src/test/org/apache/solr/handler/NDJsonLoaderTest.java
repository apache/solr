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
package org.apache.solr.handler;

import static org.hamcrest.Matchers.containsString;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.loader.NDJsonLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.junit.Test;

public class NDJsonLoaderTest extends SolrTestCase {

  private static BufferingRequestProcessor load(String content, SolrParams params)
      throws Exception {
    BufferingRequestProcessor processor = new BufferingRequestProcessor(null);
    try (SolrQueryRequest req = new SolrQueryRequestBase(null, params)) {
      new NDJsonLoader()
          .load(
              req, new SolrQueryResponse(), new ContentStreamBase.StringStream(content), processor);
    }
    return processor;
  }

  private static BufferingRequestProcessor load(String content) throws Exception {
    return load(content, new ModifiableSolrParams());
  }

  @Test
  public void testOneDocPerLine() throws Exception {
    BufferingRequestProcessor p =
        load("{\"id\":\"1\",\"title\":\"one\"}\n{\"id\":\"2\",\"title\":[\"a\",\"b\"]}\n");

    assertEquals(2, p.addCommands.size());
    assertEquals(
        "SolrInputDocument(fields: [id=1, title=one])", p.addCommands.get(0).solrDoc.toString());
    assertEquals(
        "SolrInputDocument(fields: [id=2, title=[a, b]])", p.addCommands.get(1).solrDoc.toString());
    assertTrue(p.deleteCommands.isEmpty());
    assertTrue(p.commitCommands.isEmpty());
  }

  @Test
  public void testTolerantOfBlankLinesAndMissingTrailingNewline() throws Exception {
    BufferingRequestProcessor p = load("\n{\"id\":\"1\"}\n\n   \n{\"id\":\"2\"}");
    assertEquals(2, p.addCommands.size());
  }

  @Test
  public void testChildDocuments() throws Exception {
    BufferingRequestProcessor p = load("{\"id\":\"1\",\"children\":[{\"id\":\"1a\"}]}\n");
    assertEquals(1, p.addCommands.size());
    assertEquals(
        "SolrInputDocument(fields: [id=1, children=[SolrInputDocument(fields: [id=1a])]])",
        p.addCommands.get(0).solrDoc.toString());
  }

  @Test
  public void testUpdateParamsApply() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("commitWithin", "1234");
    params.set("overwrite", "false");

    AddUpdateCommand add = load("{\"id\":\"1\"}\n", params).addCommands.get(0);
    assertEquals(1234, add.commitWithin);
    assertFalse(add.overwrite);
  }

  @Test
  public void testErrorReportsLineNumber() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\"}\n{\"id\":\n{\"id\":\"3\"}\n"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("line 2"));
  }

  @Test
  public void testRejectsNonObjectLine() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\"}\n[{\"id\":\"2\"}]\n"));
    assertThat(e.getMessage(), containsString("line 2"));
    assertThat(e.getMessage(), containsString("expected a JSON object"));
  }

  @Test
  public void testRejectsMultipleObjectsOnOneLine() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\"} {\"id\":\"2\"}\n"));
    assertThat(e.getMessage(), containsString("exactly one JSON object per line"));
  }
}
