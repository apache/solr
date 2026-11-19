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
package org.apache.solr.response.transform;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.highlight.UnifiedSolrHighlighter;
import org.apache.solr.highlight.UnifiedSolrHighlighter.SolrExtendedUnifiedHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.ResultContext;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterationInfo;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;

/**
 * Highlights one field of each document with the {@link UnifiedHighlighter}, adding the snippets as
 * a list under the transformer's name, e.g. {@code body_hl:[hl f=body]}. Without {@code f}, the
 * field is the transformer's name, e.g. {@code body:[hl]}. When the name is the field and nothing
 * is highlighted, the field's original value is returned instead.
 *
 * <p>Other local params are {@code hl.*} params without the {@code hl.} prefix (e.g. {@code
 * snippets}, {@code tag.pre}, {@code q}); they override the request's {@code hl.*} params, which
 * otherwise apply.
 */
public class HighlightAugmenterFactory extends TransformerFactory {

  /** Local param naming the field to highlight. */
  public static final String FIELD_PARAM = "f";

  private static final String HL_PREFIX = HighlightParams.HIGHLIGHT + ".";

  /** Names of the {@code hl.*} params in {@link HighlightParams}, without the prefix. */
  private static final Set<String> HL_PARAM_NAMES =
      Arrays.stream(HighlightParams.class.getFields())
          .filter(f -> f.getType() == String.class && Modifier.isStatic(f.getModifiers()))
          .map(
              f -> {
                try {
                  return (String) f.get(null);
                } catch (IllegalAccessException e) {
                  throw new IllegalStateException(e);
                }
              })
          .filter(name -> name.startsWith(HL_PREFIX))
          .map(name -> name.substring(HL_PREFIX.length()))
          .collect(Collectors.toUnmodifiableSet());

  @Override
  public DocTransformer create(String display, SolrParams localParams, SolrQueryRequest req) {
    String field = localParams.get(FIELD_PARAM, display);
    SchemaField schemaField = req.getSchema().getFieldOrNull(field);
    if (schemaField == null) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "[hl] needs a field to highlight, named by the '"
              + FIELD_PARAM
              + "' param or by its alias; not a field: "
              + field);
    }

    ModifiableSolrParams overrides = new ModifiableSolrParams();
    for (Iterator<String> it = localParams.getParameterNamesIterator(); it.hasNext(); ) {
      String name = it.next();
      if (name.equals(FIELD_PARAM)) {
        continue;
      }
      if (!HL_PARAM_NAMES.contains(name)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown [hl] param: " + name);
      }
      String[] values = localParams.getParams(name);
      overrides.set(HL_PREFIX + name, values);
      // per-field form too, so that it overrides a request's per-field param
      overrides.set("f." + field + "." + HL_PREFIX + name, values);
    }
    SolrParams params = SolrParams.wrapDefaults(overrides, req.getParams());

    boolean passThrough = display.equals(field);

    boolean singleValue =
        !schemaField.multiValued()
            && params.getFieldInt(
                    field, HighlightParams.SNIPPETS, UnifiedSolrHighlighter.DEFAULT_SNIPPETS)
                == 1;

    return new HighlightAugmenter(
        display, field, params, parseHighlightQuery(params, req), passThrough, singleValue);
  }

  /** Parses {@code hl.q}, as the highlight component does; null if absent. */
  private static Query parseHighlightQuery(SolrParams params, SolrQueryRequest req) {
    String hlq = params.get(HighlightParams.Q);
    if (hlq == null) {
      return null;
    }
    String parserName =
        params.get(
            HighlightParams.QPARSER, params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE));
    try {
      return QParser.getParser(hlq, parserName, req).getHighlightQuery();
    } catch (SyntaxError e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
  }

  static class HighlightAugmenter extends DocTransformer {
    private final String name;
    private final String field;
    private final SolrParams params;
    private final Query hlQuery;
    private final boolean passThrough;
    private final boolean singleValue; // output a value instead of a list
    private final SolrReturnFields originalValueFields;
    private SingleFieldHighlighter highlighter; // null if there's no query

    HighlightAugmenter(
        String name,
        String field,
        SolrParams params,
        Query hlQuery,
        boolean passThrough,
        boolean singleValue) {
      this.name = name;
      this.field = field;
      this.params = params;
      this.hlQuery = hlQuery;
      this.passThrough = passThrough;
      this.singleValue = singleValue;
      this.originalValueFields = passThrough ? new SolrReturnFields(List.of(field), null) : null;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean needsSolrIndexSearcher() {
      return true;
    }

    @Override
    public void setContext(ResultContext context) {
      super.setContext(context);
      Query query = hlQuery != null ? hlQuery : getMainHighlightQuery(context);
      SolrIndexSearcher searcher = context.getSearcher();
      highlighter =
          (query == null || searcher == null)
              ? null
              : new SingleFieldHighlighter(searcher, params, field, query);
    }

    // Identity is intended: the context must come from this exact request and query.
    @SuppressWarnings("ReferenceEquality")
    private static Query getMainHighlightQuery(ResultContext context) {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      ResponseBuilder rb = info == null ? null : info.getResponseBuilder();
      // A custom result context may have a different query from the current request.
      if (rb == null
          || rb.req != context.getRequest()
          || rb.getQuery() != context.getQuery()
          || rb.getQparser() == null) {
        return context.getQuery();
      }
      try {
        return rb.getQparser().getHighlightQuery();
      } catch (SyntaxError e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    }

    @Override
    public void transform(SolrDocument doc, int docid, DocIterationInfo docInfo)
        throws IOException {
      if (docid < 0) {
        return; // not from the index
      }
      Object highlighted = highlighter == null ? null : highlighter.highlight(docid);
      List<String> snippets =
          highlighted == null
              ? List.of()
              // a multi-valued field's value separator can yield empty snippets
              : Arrays.stream(UnifiedSolrHighlighter.splitSnippets(highlighted.toString()))
                  .filter(s -> !s.isEmpty())
                  .toList();
      if (!snippets.isEmpty()) {
        doc.setField(name, singleValue ? snippets.getFirst() : snippets);
      } else if (passThrough) {
        Object original =
            context.getDocFetcher().solrDoc(docid, originalValueFields).getFieldValue(field);
        if (original != null) {
          doc.setField(name, original);
        }
      } else if (!singleValue) {
        doc.setField(name, List.of());
      }
    }
  }

  /**
   * Highlights one field of one document at a time. Query analysis and per-field setup happen once,
   * in the constructor.
   */
  private static final class SingleFieldHighlighter extends SolrExtendedUnifiedHighlighter {
    private final String[] fields;
    private final FieldHighlighter fieldHighlighter;

    SingleFieldHighlighter(
        SolrIndexSearcher searcher, SolrParams params, String field, Query query) {
      super(searcher, searcher.getSchema(), params, null);
      this.fields = new String[] {field};
      int maxPassages =
          params.getFieldInt(
              field, HighlightParams.SNIPPETS, UnifiedSolrHighlighter.DEFAULT_SNIPPETS);
      this.fieldHighlighter = getFieldHighlighter(field, query, extractTerms(query), maxPassages);
    }

    /** Returns the formatted highlight of the doc's field, or null if none. */
    Object highlight(int docId) throws IOException {
      CharSequence content =
          loadFieldValues(fields, DocIdSetIterator.range(docId, docId + 1), 0).getFirst()[0];
      if (content == null) {
        return null;
      }
      List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
      LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(docId, leaves));
      return fieldHighlighter.highlightFieldForDoc(
          leaf.reader(), docId - leaf.docBase, content.toString());
    }
  }
}
