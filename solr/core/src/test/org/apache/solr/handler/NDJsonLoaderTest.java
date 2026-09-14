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

import java.util.Locale;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.loader.ContentStreamLoader;
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
    return load(new NDJsonLoader(), content, params);
  }

  private static BufferingRequestProcessor load(
      ContentStreamLoader loader, String content, SolrParams params) throws Exception {
    BufferingRequestProcessor processor = new BufferingRequestProcessor(null);
    try (SolrQueryRequest req = new SolrQueryRequestBase(null, params)) {
      loader.load(
          req, new SolrQueryResponse(), new ContentStreamBase.StringStream(content), processor);
    }
    return processor;
  }

  private static BufferingRequestProcessor loadWithContentType(String content, String contentType)
      throws Exception {
    BufferingRequestProcessor processor = new BufferingRequestProcessor(null);
    ContentStreamBase.StringStream stream = new ContentStreamBase.StringStream(content);
    stream.setContentType(contentType);
    try (SolrQueryRequest req = new SolrQueryRequestBase(null, new ModifiableSolrParams())) {
      new NDJsonLoader().load(req, new SolrQueryResponse(), stream, processor);
    }
    return processor;
  }

  private static ContentStreamLoader loaderWithMaxLineLength(int maxLineLength) {
    ModifiableSolrParams args = new ModifiableSolrParams();
    args.set("maxLineLength", maxLineLength);
    return new NDJsonLoader().init(args);
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

  /** Input that is not really newline delimited must fail, not buffer onto the heap. */
  @Test
  public void testOverlongLineIsRejected() {
    String oneLongLine = "[" + "{\"id\":\"1\"},".repeat(500) + "{\"id\":\"x\"}]";
    SolrException e =
        expectThrows(
            SolrException.class,
            () -> load(loaderWithMaxLineLength(256), oneLongLine, new ModifiableSolrParams()));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("maxLineLength"));
  }

  /** The default budget scales with the heap, so it is a backstop rather than a document limit. */
  @Test
  public void testDefaultMaxLineLengthScalesWithHeap() {
    int expected =
        Math.clamp(
            Runtime.getRuntime().maxMemory() / 32,
            NDJsonLoader.MIN_DEFAULT_MAX_LINE_LENGTH,
            Integer.MAX_VALUE);
    assertEquals(expected, NDJsonLoader.defaultMaxLineLength());
    assertTrue(
        "default must never drop below the floor",
        NDJsonLoader.defaultMaxLineLength() >= NDJsonLoader.MIN_DEFAULT_MAX_LINE_LENGTH);
  }

  /** The limit can be lowered globally, without redefining the implicit update handlers. */
  @Test
  public void testMaxLineLengthSystemProperty() {
    System.setProperty(NDJsonLoader.MAX_LINE_LENGTH_PROP, "64");
    try {
      SolrException e =
          expectThrows(
              SolrException.class,
              () ->
                  load(
                      new NDJsonLoader(),
                      "{\"id\":\"1\",\"title_s\":\"" + "x".repeat(200) + "\"}\n",
                      new ModifiableSolrParams()));
      assertThat(e.getMessage(), containsString("maxLineLength of 64"));
    } finally {
      System.clearProperty(NDJsonLoader.MAX_LINE_LENGTH_PROP);
    }
  }

  /** An explicit handler arg wins over the system property. */
  @Test
  public void testHandlerArgOverridesSystemProperty() throws Exception {
    System.setProperty(NDJsonLoader.MAX_LINE_LENGTH_PROP, "8");
    try {
      BufferingRequestProcessor p =
          load(loaderWithMaxLineLength(4096), "{\"id\":\"1\"}\n", new ModifiableSolrParams());
      assertEquals(1, p.addCommands.size());
    } finally {
      System.clearProperty(NDJsonLoader.MAX_LINE_LENGTH_PROP);
    }
  }

  /** The budget is per line, so any number of lines under the limit must be accepted. */
  @Test
  public void testManyLinesEachUnderTheLimit() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      sb.append("{\"id\":\"").append(i).append("\",\"title_s\":\"padding padding padding\"}\n");
    }
    assertTrue("test input should far exceed the limit in total", sb.length() > 100_000);
    BufferingRequestProcessor p =
        load(loaderWithMaxLineLength(256), sb.toString(), new ModifiableSolrParams());
    assertEquals(5000, p.addCommands.size());
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

  @Test
  public void testRejectsMultipleObjectsOnOneLine() {
    SolrException e =
        expectThrows(SolrException.class, () -> load("{\"id\":\"1\"} {\"id\":\"2\"}\n"));
    assertThat(e.getMessage(), containsString("exactly one JSON object per line"));
  }
}
