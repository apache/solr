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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.security.PermissionNameProvider.Name;
import org.junit.BeforeClass;
import org.junit.Test;

public class HighlightRequestHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testTextPlainResponseShapeWithoutIndexedDocuments() throws Exception {
    ModifiableSolrParams params =
        paramsSet(CommonParams.Q, "text:solr", HighlightParams.FIELDS, "text");
    SolrQueryResponse response =
        handle(
            params,
            List.of(new ContentStreamBase.StringStream("Solr highlights the text.", "text/plain")));

    assertArrayEquals(
        new String[] {"<em>Solr</em> highlights the text."}, snippets(response, "text"));
  }

  @Test
  public void testGetStreamBodyAndHighlightQueryParameter() throws Exception {
    ModifiableSolrParams params =
        paramsSet(
            HighlightParams.Q,
            "text:solr",
            HighlightParams.FIELDS,
            "text",
            "stream.body",
            "Solr highlights the text.");

    SolrQueryResponse response = handle(params, null);

    assertArrayEquals(
        new String[] {"<em>Solr</em> highlights the text."}, snippets(response, "text"));
  }

  @Test
  public void testDefaultFieldFallback() throws Exception {
    ModifiableSolrParams params = paramsSet(CommonParams.Q, "solr", CommonParams.DF, "text");
    SolrQueryResponse response =
        handle(
            params,
            List.of(new ContentStreamBase.StringStream("Solr highlights the text.", "text/plain")));

    assertArrayEquals(
        new String[] {"<em>Solr</em> highlights the text."}, snippets(response, "text"));
  }

  @Test
  public void testHtmlEncoderEscapesSuppliedText() throws Exception {
    ModifiableSolrParams params =
        paramsSet(
            CommonParams.Q,
            "text:solr",
            HighlightParams.FIELDS,
            "text",
            HighlightParams.ENCODER,
            "html");

    SolrQueryResponse response =
        handle(
            params,
            List.of(
                new ContentStreamBase.StringStream(
                    "<strong>Solr</strong>", "text/plain; charset=UTF-8")));

    assertArrayEquals(
        new String[] {"&lt;strong&gt;<em>Solr</em>&lt;&#x2F;strong&gt;"}, snippets(response, "text"));
  }

  @Test
  public void testPhraseAndMultiTermQueries() throws Exception {
    List<ContentStream> content =
        List.of(new ContentStreamBase.StringStream("Solr highlights the text.", "text/plain"));
    ModifiableSolrParams phraseParams =
        paramsSet(CommonParams.Q, "text:\"solr highlights\"", HighlightParams.FIELDS, "text");
    SolrQueryResponse phraseResponse = handle(phraseParams, content);
    assertArrayEquals(
        new String[] {"<em>Solr highlights</em> the text."}, snippets(phraseResponse, "text"));

    ModifiableSolrParams nonPhraseParams =
        paramsSet(
            CommonParams.Q,
            "text:\"solr highlights\"",
            HighlightParams.FIELDS,
            "text",
            HighlightParams.USE_PHRASE_HIGHLIGHTER,
            "false");
    SolrQueryResponse nonPhraseResponse = handle(nonPhraseParams, content);
    assertArrayEquals(
        new String[] {"<em>Solr</em> <em>highlights</em> the text."},
        snippets(nonPhraseResponse, "text"));

    ModifiableSolrParams prefixParams =
        paramsSet(CommonParams.Q, "text:sol*", HighlightParams.FIELDS, "text");
    SolrQueryResponse prefixResponse = handle(prefixParams, content);
    assertArrayEquals(
        new String[] {"<em>Solr</em> highlights the text."}, snippets(prefixResponse, "text"));
  }

  @Test
  public void testDefaultSummaryReturnsWholeInputWhenThereIsNoMatch() throws Exception {
    ModifiableSolrParams params =
        paramsSet(
            CommonParams.Q,
            "text:missing",
            HighlightParams.FIELDS,
            "text",
            HighlightParams.DEFAULT_SUMMARY,
            "true");
    SolrQueryResponse response =
        handle(
            params,
            List.of(new ContentStreamBase.StringStream("Solr highlights the text.", "text/plain")));

    assertArrayEquals(new String[] {"Solr highlights the text."}, snippets(response, "text"));
  }

  @Test
  public void testLengthLimitTruncatesAndRejectsOversizedLimit() throws Exception {
    ModifiableSolrParams params =
        paramsSet(
            CommonParams.Q,
            "text:solr",
            HighlightParams.FIELDS,
            "text",
            HighlightParams.MAX_CHARS,
            "4");
    CountingReader countingReader = new CountingReader("Solr highlights");
    ContentStream contentStream =
        new ContentStreamBase.StringStream("Solr highlights", "text/plain") {
          @Override
          public Reader getReader() {
            return countingReader;
          }
        };
    SolrQueryResponse response = handle(params, List.of(contentStream));
    assertArrayEquals(new String[] {"<em>Solr</em>"}, snippets(response, "text"));
    assertEquals(4, countingReader.getCharsRead());

    params.set(HighlightParams.MAX_CHARS, "1048577");
    SolrException exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(params, List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertTrue(exception.getMessage().contains("must not exceed"));
  }

  @Test
  public void testRejectsInvalidInputs() throws Exception {
    ModifiableSolrParams params =
        paramsSet(CommonParams.Q, "text:solr", HighlightParams.FIELDS, "text");
    SolrException exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    params,
                    List.of(new ContentStreamBase.StringStream("Solr", "application/json"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());

    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    params,
                    List.of(
                        new ContentStreamBase.StringStream("one", "text/plain"),
                        new ContentStreamBase.StringStream("two", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());

    exception = expectThrows(SolrException.class, () -> handle(params, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());

    ModifiableSolrParams noQuery = paramsSet(HighlightParams.FIELDS, "text");
    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(noQuery, List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());

    ModifiableSolrParams noField = paramsSet(CommonParams.Q, "text:solr");
    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(noField, List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());

    ModifiableSolrParams wildcardField =
        paramsSet(CommonParams.Q, "text:solr", HighlightParams.FIELDS, "text*");
    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    wildcardField,
                    List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());

    ModifiableSolrParams invalidLimit =
        paramsSet(
            CommonParams.Q,
            "text:solr",
            HighlightParams.FIELDS,
            "text",
            HighlightParams.MAX_CHARS,
            "0");
    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    invalidLimit,
                    List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
  }

  @Test
  public void testRejectsParserOverridesAndParserSubqueries() throws Exception {
    ModifiableSolrParams parserOverride =
        paramsSet(
            CommonParams.Q,
            "text:solr",
            HighlightParams.FIELDS,
            "text",
            HighlightParams.QPARSER,
            "mlt");
    SolrException exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    parserOverride,
                    List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertTrue(exception.getMessage().contains("supports only"));

    ModifiableSolrParams defTypeOverride =
        paramsSet(CommonParams.Q, "text:solr", HighlightParams.FIELDS, "text", "defType", "mlt");
    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    defTypeOverride,
                    List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertTrue(exception.getMessage().contains("supports only"));

    ModifiableSolrParams localParamsQuery =
        paramsSet(CommonParams.Q, "{!mlt qf=text}solr", HighlightParams.FIELDS, "text");
    exception =
        expectThrows(
            SolrException.class,
            () ->
                handle(
                    localParamsQuery,
                    List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertTrue(exception.getMessage().contains("Local-parameter"));

    ModifiableSolrParams subquery =
        paramsSet(
            CommonParams.Q,
            "text:solr OR _query_:\"{!mlt qf=text}solr\"",
            HighlightParams.FIELDS,
            "text");
    SolrException subqueryException =
        expectThrows(
            SolrException.class,
            () -> handle(subquery, List.of(new ContentStreamBase.StringStream("Solr", "text/plain"))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, subqueryException.code());
  }

  @Test
  public void testRegistrationAndPermission() {
    RequestHandlerBase handler = newHandler();
    assertTrue(handler.registerV2());
    assertEquals(Name.READ_PERM, handler.getPermissionName(null));
  }

  private static SolrQueryResponse handle(
      ModifiableSolrParams params, List<ContentStream> contentStreams) throws Exception {
    RequestHandlerBase handler = newHandler();
    try (SolrQueryRequestBase request = new NoSearcherRequest(params)) {
      if (contentStreams != null) {
        request.setContentStreams(contentStreams);
      }
      SolrQueryResponse response = new SolrQueryResponse();
      handler.handleRequestBody(request, response);
      return response;
    }
  }

  private static RequestHandlerBase newHandler() {
    try {
      return Class.forName("org.apache.solr.handler.HighlightRequestHandler")
          .asSubclass(RequestHandlerBase.class)
          .getConstructor()
          .newInstance();
    } catch (ReflectiveOperationException e) {
      throw new LinkageError("The text highlight request handler must be available", e);
    }
  }

  private static String[] snippets(SolrQueryResponse response, String field) {
    @SuppressWarnings("unchecked")
    NamedList<Object> highlighting = (NamedList<Object>) response.getValues().get("highlighting");
    @SuppressWarnings("unchecked")
    NamedList<Object> document = (NamedList<Object>) highlighting.get("text");
    return (String[]) document.get(field);
  }

  private static ModifiableSolrParams paramsSet(String... nameValues) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int i = 0; i < nameValues.length; i += 2) {
      params.set(nameValues[i], nameValues[i + 1]);
    }
    return params;
  }

  private static class NoSearcherRequest extends SolrQueryRequestBase {
    NoSearcherRequest(ModifiableSolrParams params) {
      super(h.getCore(), params);
    }

    @Override
    public SolrIndexSearcher getSearcher() {
      throw new AssertionError("Text highlighting must not acquire an index searcher");
    }
  }

  private static class CountingReader extends Reader {
    private final StringReader delegate;
    private int charsRead;

    CountingReader(String text) {
      delegate = new StringReader(text);
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      int read = delegate.read(buffer, offset, length);
      if (read > 0) {
        charsRead += read;
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    int getCharsRead() {
      return charsRead;
    }
  }
}
