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
import java.text.BreakIterator;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.uhighlight.DefaultPassageFormatter;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.search.uhighlight.WholeBreakIterator;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.highlight.UnifiedSolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.security.AuthorizationContext;

/** Highlights supplied plain text with a query. */
public class HighlightRequestHandler extends RequestHandlerBase {

  private static final String TEXT_RESPONSE_KEY = "text";
  private static final String STREAM_BODY_PARAM = "stream.body";
  static final int MAX_ALLOWED_CHARS = 1_048_576;

  @Override
  public String getDescription() {
    return "Highlights supplied text with a query.";
  }

  @Override
  public Boolean registerV2() {
    return true;
  }

  @Override
  public Collection<Api> getApis() {
    return Set.of(
        new ApiBag.ReqHandlerToApi(
            this,
            () -> {
              var spec = ApiBag.HANDLER_NAME_SPEC_PROVIDER.getSpec();
              spec.put("methods", List.of("GET", "POST", "QUERY"));
              return spec;
            }));
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    int maxChars = params.getInt(HighlightParams.MAX_CHARS, SolrHighlighter.DEFAULT_MAX_CHARS);
    if (maxChars < 1) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          HighlightParams.MAX_CHARS + " must be a positive integer");
    }
    if (maxChars > MAX_ALLOWED_CHARS) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          HighlightParams.MAX_CHARS + " must not exceed " + MAX_ALLOWED_CHARS);
    }

    String text = readText(req, maxChars);
    String queryString = params.get(HighlightParams.Q, params.get(CommonParams.Q));
    if (queryString == null || queryString.isBlank()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "A query is required using '" + CommonParams.Q + "' or '" + HighlightParams.Q + "'");
    }

    String parserType = params.get(HighlightParams.QPARSER, params.get(QueryParsing.DEFTYPE));
    if (parserType != null && !QParserPlugin.DEFAULT_QTYPE.equals(parserType)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "HighlightRequestHandler supports only the '"
              + QParserPlugin.DEFAULT_QTYPE
              + "' query parser");
    }
    if (queryString.stripLeading().startsWith("{!")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Local-parameter query parsers are not supported by HighlightRequestHandler");
    }
    QParser parser = createQueryParser(queryString, req);
    Query query = parser.getHighlightQuery();
    if (query == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "The query did not produce a highlight query");
    }

    rejectWildcardFields(params.getParams(HighlightParams.FIELDS));
    rejectWildcardFields(parser.getDefaultHighlightFields());
    String[] fields =
        new UnifiedSolrHighlighter()
            .getHighlightFields(query, req, parser.getDefaultHighlightFields());
    rejectWildcardFields(fields);
    if (fields.length == 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "A field is required using '"
              + HighlightParams.FIELDS
              + "' or '"
              + CommonParams.DF
              + "'");
    }

    UnifiedHighlighter highlighter = createHighlighter(req, maxChars);
    NamedList<Object> fieldsToSnippets = new SimpleOrderedMap<>();
    for (String field : fields) {
      int maxPassages = params.getFieldInt(field, HighlightParams.SNIPPETS, 1);
      Object highlightedText =
          highlighter.highlightWithoutSearcher(field, query, text, maxPassages);
      if (highlightedText != null) {
        fieldsToSnippets.add(field, new String[] {(String) highlightedText});
      }
    }

    NamedList<Object> documentHighlights = new SimpleOrderedMap<>();
    documentHighlights.add(TEXT_RESPONSE_KEY, fieldsToSnippets);
    rsp.add("highlighting", documentHighlights);
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.READ_PERM;
  }

  private static String readText(SolrQueryRequest req, int maxChars) throws IOException {
    Iterable<ContentStream> contentStreams = req.getContentStreams();
    if (contentStreams != null) {
      var iterator = contentStreams.iterator();
      if (iterator.hasNext()) {
        ContentStream contentStream = iterator.next();
        if (iterator.hasNext()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "HighlightRequestHandler accepts one text/plain content stream");
        }
        String contentType = contentStream.getContentType();
        String mediaType = contentType == null ? null : contentType.split(";", 2)[0].trim();
        if (!"text/plain".equalsIgnoreCase(mediaType)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "HighlightRequestHandler requires a text/plain content stream");
        }
        try (Reader reader = contentStream.getReader()) {
          return readUpTo(reader, maxChars);
        }
      }
    }

    String streamBody = req.getParams().get(STREAM_BODY_PARAM);
    if (streamBody != null) {
      return streamBody.length() <= maxChars ? streamBody : streamBody.substring(0, maxChars);
    }
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "HighlightRequestHandler requires text/plain content or the '"
            + STREAM_BODY_PARAM
            + "' parameter");
  }

  private static String readUpTo(Reader reader, int maxChars) throws IOException {
    char[] buffer = new char[Math.min(maxChars, 8192)];
    StringBuilder text = new StringBuilder(buffer.length);
    int remaining = maxChars;
    int read;
    while (remaining > 0
        && (read = reader.read(buffer, 0, Math.min(remaining, buffer.length))) != -1) {
      text.append(buffer, 0, read);
      remaining -= read;
    }
    return text.toString();
  }

  private static void rejectWildcardFields(String[] fields) {
    if (fields == null) return;
    for (String field : fields) {
      if (field != null && field.contains("*")) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Wildcard fields are not supported because highlighting does not access the index");
      }
    }
  }

  private static UnifiedHighlighter createHighlighter(SolrQueryRequest req, int maxChars) {
    SolrParams params = req.getParams();
    UnifiedHighlighter.Builder builder =
        UnifiedHighlighter.builderWithoutSearcher(req.getSchema().getIndexAnalyzer())
            .withMaxLength(maxChars)
            .withBreakIterator(WholeBreakIterator::new)
            .withHandleMultiTermQuery(true)
            .withHighlightPhrasesStrictly(true)
            .withPassageRelevancyOverSpeed(true)
            .withWeightMatches(true);

    return new UnifiedHighlighter(builder) {
      @Override
      protected BreakIterator getBreakIterator(String field) {
        return new WholeBreakIterator();
      }

      @Override
      protected int getMaxNoHighlightPassages(String field) {
        return params.getFieldBool(field, HighlightParams.DEFAULT_SUMMARY, false) ? -1 : 0;
      }

      @Override
      protected PassageFormatter getFormatter(String field) {
        String preTag =
            params.getFieldParam(
                field,
                HighlightParams.TAG_PRE,
                params.getFieldParam(field, HighlightParams.SIMPLE_PRE, "<em>"));
        String postTag =
            params.getFieldParam(
                field,
                HighlightParams.TAG_POST,
                params.getFieldParam(field, HighlightParams.SIMPLE_POST, "</em>"));
        String encoder = params.getFieldParam(field, HighlightParams.ENCODER, "simple");
        return new DefaultPassageFormatter(preTag, postTag, "", "html".equals(encoder));
      }

      @Override
      protected Set<HighlightFlag> getFlags(String field) {
        Set<HighlightFlag> flags = EnumSet.noneOf(HighlightFlag.class);
        flags.addAll(super.getFlags(field));
        if (!params.getFieldBool(field, HighlightParams.HIGHLIGHT_MULTI_TERM, true)) {
          flags.remove(HighlightFlag.MULTI_TERM_QUERY);
        }
        if (!params.getFieldBool(field, HighlightParams.USE_PHRASE_HIGHLIGHTER, true)) {
          flags.remove(HighlightFlag.PHRASES);
        }
        if (!flags.contains(HighlightFlag.MULTI_TERM_QUERY)
            || !flags.contains(HighlightFlag.PHRASES)) {
          flags.remove(HighlightFlag.WEIGHT_MATCHES);
        }
        return flags;
      }

      @Override
      protected Predicate<String> getFieldMatcher(String field) {
        return params.getFieldBool(field, HighlightParams.FIELD_MATCH, false)
            ? field::equals
            : ignored -> true;
      }
    };
  }

  private static QParser createQueryParser(String queryString, SolrQueryRequest req) {
    // Do not use QParser.getParser here: configured plugins and local parameters can dispatch to
    // parsers that use the index. Keep this parser limited to the built-in Lucene syntax.
    return new QParser(queryString, null, req.getParams(), req) {
      private SolrQueryParser parser;

      @Override
      public Query parse() throws SyntaxError {
        if (qstr == null || qstr.isEmpty()) return null;

        parser = new SolrQueryParser(this, getParam(CommonParams.DF));
        parser.setDefaultOperator(QueryParsing.parseOP(getParam(QueryParsing.OP)));
        parser.setSplitOnWhitespace(
            StrUtils.parseBool(
                getParam(QueryParsing.SPLIT_ON_WHITESPACE),
                SolrQueryParser.DEFAULT_SPLIT_ON_WHITESPACE));
        parser.setAllowSubQueryParsing(false);
        return parser.parse(qstr);
      }

      @Override
      public String[] getDefaultHighlightFields() {
        return parser == null ? new String[] {} : new String[] {parser.getDefaultField()};
      }
    };
  }
}
