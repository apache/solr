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

import java.util.List;
import java.util.Locale;
import java.util.Map;
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

  private static BufferingRequestProcessor load(String content) throws Exception {
    return load(content, new ModifiableSolrParams());
  }

  private static BufferingRequestProcessor load(String content, SolrParams params)
      throws Exception {
    return load(content, params, new SolrQueryResponse(), null);
  }

  private static BufferingRequestProcessor loadWithContentType(String content, String contentType)
      throws Exception {
    return load(content, new ModifiableSolrParams(), new SolrQueryResponse(), contentType);
  }

  private static BufferingRequestProcessor load(
      String content, SolrParams params, SolrQueryResponse rsp, String contentType)
      throws Exception {
    BufferingRequestProcessor processor = new BufferingRequestProcessor(null);
    ContentStreamBase.StringStream stream = new ContentStreamBase.StringStream(content);
    if (contentType != null) {
      stream.setContentType(contentType);
    }
    try (SolrQueryRequest req = new SolrQueryRequestBase(null, params)) {
      new NDJsonLoader().load(req, rsp, stream, processor);
    }
    return processor;
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
  public void testCarriageReturnLineEndings() throws Exception {
    BufferingRequestProcessor p = load("{\"id\":\"1\"}\r\n{\"id\":\"2\"}\r\n");
    assertEquals(2, p.addCommands.size());
    assertEquals("SolrInputDocument(fields: [id=1])", p.addCommands.get(0).solrDoc.toString());
    assertEquals("SolrInputDocument(fields: [id=2])", p.addCommands.get(1).solrDoc.toString());
  }

  @Test
  public void testTolerantOfBlankLinesAndMissingTrailingNewline() throws Exception {
    BufferingRequestProcessor p = load("\n{\"id\":\"1\"}\n\n   \n{\"id\":\"2\"}");
    assertEquals(2, p.addCommands.size());
  }

  /** A nested object flattens to dotted field names, as on the /update/json/docs path. */
  @Test
  public void testNestedObjectIsFlattenedByDefault() throws Exception {
    BufferingRequestProcessor p = load("{\"id\":\"1\",\"children\":[{\"id\":\"1a\"}]}\n");
    assertEquals(1, p.addCommands.size());
    assertEquals(
        "SolrInputDocument(fields: [id=1, children.id=1a])",
        p.addCommands.get(0).solrDoc.toString());
  }

  /** Nesting is declared with split, and the root must be listed first. */
  @Test
  public void testChildDocumentsViaSplit() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("split", "/|/children");
    BufferingRequestProcessor p =
        load("{\"id\":\"1\",\"children\":[{\"id\":\"1a\"},{\"id\":\"1b\"}]}\n", params);
    assertEquals(1, p.addCommands.size());
    assertEquals(
        "SolrInputDocument(fields: [id=1, children=[SolrInputDocument(fields: [id=1a]), "
            + "SolrInputDocument(fields: [id=1b])]])",
        p.addCommands.get(0).solrDoc.toString());
  }

  /** Each line is already a document, so a split that skips the root makes no sense. */
  @Test
  public void testSplitMustStartAtRoot() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("split", "/children");
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\",\"children\":[]}\n", params));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("must start at the document root"));
  }

  @Test
  public void testSplitRejectsWildcards() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("split", "/|/a/*");
    SolrException e = expectThrows(SolrException.class, () -> load("{\"id\":\"1\"}\n", params));
    assertThat(e.getMessage(), containsString("wildcards"));
  }

  /** srcField needs the recording parser, which NDJSON replaces with its own. */
  @Test
  public void testSrcFieldIsRejected() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("srcField", "_src_");
    SolrException e = expectThrows(SolrException.class, () -> load("{\"id\":\"1\"}\n", params));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("srcField is not supported"));
  }

  /** echo is inherited from the JSON docs path: documents are returned, not indexed. */
  @Test
  public void testEcho() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("echo", "true");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor processor =
        load("{\"id\":\"1\"}\n{\"id\":\"2\"}\n", params, rsp, null);
    assertTrue(processor.addCommands.isEmpty());
    assertEquals(List.of(Map.of("id", "1"), Map.of("id", "2")), rsp.getValues().get("docs"));
  }

  /** Field mappings still apply, so a caller can rename or select what gets indexed. */
  @Test
  public void testFieldMapping() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("f", "id:/id");
    params.add("f", "title_s:/name");
    BufferingRequestProcessor p =
        load("{\"id\":\"1\",\"name\":\"one\",\"ignored\":\"x\"}\n", params);
    assertEquals(
        "SolrInputDocument(fields: [id=1, title_s=one])", p.addCommands.get(0).solrDoc.toString());
  }

  /** A document spanning lines breaks the format, and is reported where it starts. */
  @Test
  public void testDocumentSpanningLinesIsRejected() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\"}\n{\n\"id\":\"2\"}\n"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("must be on a single line"));
    assertThat(e.getMessage(), containsString("line 2"));
  }

  /** A whole JSON array is the classic "not really newline delimited" input. */
  @Test
  public void testTopLevelArrayIsRejected() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("[{\"id\":\"1\"},{\"id\":\"2\"}]\n"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("no enclosing array"));
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

  /** Records stream as they are parsed, so documents before a bad line are already submitted. */
  @Test
  public void testDocumentsBeforeAFailureAreSubmitted() {
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    expectThrows(
        SolrException.class,
        () -> {
          try (SolrQueryRequest req = new SolrQueryRequestBase(null, new ModifiableSolrParams())) {
            new NDJsonLoader()
                .load(
                    req,
                    new SolrQueryResponse(),
                    new ContentStreamBase.StringStream("{\"id\":\"1\"}\nnot json\n"),
                    p);
          }
        });
    assertEquals(1, p.addCommands.size());
  }

  /** NDJSON is defined as UTF-8, and a charset saying so is accepted under any of its spellings. */
  @Test
  public void testAcceptsUtf8Charset() throws Exception {
    for (String charset : new String[] {"utf-8", "UTF-8", "utf8", "\"UTF-8\""}) {
      BufferingRequestProcessor p =
          loadWithContentType(
              "{\"id\":\"1\",\"title_s\":\"Bl\u00e5b\u00e6r\"}\n",
              "application/x-ndjson; charset=" + charset);
      assertEquals(charset, 1, p.addCommands.size());
    }
  }

  @Test
  public void testRejectsNonUtf8Charset() {
    for (String charset :
        new String[] {"iso-8859-1", "UTF-16", "windows-1252", "no-such-charset"}) {
      SolrException e =
          expectThrows(
              SolrException.class,
              () ->
                  loadWithContentType(
                      "{\"id\":\"1\"}\n", "application/x-ndjson; charset=" + charset));
      assertEquals(SolrException.ErrorCode.UNSUPPORTED_MEDIA_TYPE.code, e.code());
      assertThat(e.getMessage(), containsString("must be UTF-8"));
      assertThat(e.getMessage(), containsString(charset.toLowerCase(Locale.ROOT)));
    }
  }

  /** Line numbers are tracked across buffer refills, not just within the first one. */
  @Test
  public void testLineNumberFarIntoTheInput() {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < 3000; i++) {
      sb.append("{\"id\":\"").append(i).append("\",\"title_s\":\"padding padding\"}\n");
    }
    sb.append("not json\n");
    assertTrue("input must span many buffer fills", sb.length() > 100_000);

    SolrException e = expectThrows(SolrException.class, () -> load(sb.toString()));
    assertThat(e.getMessage(), containsString("line 3000"));
  }

  /** A single document may span buffer refills without looking like it spans lines. */
  @Test
  public void testDocumentLargerThanTheParserBuffer() throws Exception {
    String value = "x".repeat(50_000);
    BufferingRequestProcessor p =
        load("{\"id\":\"1\",\"title_s\":\"" + value + "\"}\n{\"id\":\"2\"}\n");
    assertEquals(2, p.addCommands.size());
    assertEquals(value, p.addCommands.get(0).solrDoc.getFieldValue("title_s"));
  }

  @Test
  public void testRejectsMultipleObjectsOnOneLine() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\"} {\"id\":\"2\"}\n"));
    assertThat(e.getMessage(), containsString("expected a newline between documents"));
  }
}
