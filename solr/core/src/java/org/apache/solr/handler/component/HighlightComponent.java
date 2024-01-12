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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.highlight.UnifiedSolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.SolrResponseUtil;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Highlights query words in the search results. See the <a
 * href="https://solr.apache.org/guide/solr/latest/query-guide/highlighting.html">ref guide</a>.
 *
 * @since solr 1.3
 */
public class HighlightComponent extends SearchComponent
    implements PluginInfoInitialized, SolrCoreAware {
  public enum HighlightMethod {
    UNIFIED("unified"),
    FAST_VECTOR("fastVector"),
    ORIGINAL("original");

    private static final Map<String, HighlightMethod> METHODS =
        Stream.of(values())
            .collect(
                Collectors.toUnmodifiableMap(HighlightMethod::getMethodName, Function.identity()));

    private final String methodName;

    HighlightMethod(String method) {
      this.methodName = method;
    }

    public String getMethodName() {
      return methodName;
    }

    public static HighlightMethod parse(String method) {
      return METHODS.get(method);
    }
  }

  public static final String COMPONENT_NAME = "highlight";

  protected PluginInfo info = PluginInfo.EMPTY_INFO;

  // TODO lets restructure the abstractions/relationships
  protected SolrHighlighter solrConfigHighlighter;

  @Override
  public void init(PluginInfo info) {
    this.info = info;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    rb.doHighlights = solrConfigHighlighter.isHighlightingEnabled(params);
    if (rb.doHighlights) {
      rb.setNeedDocList(true);
      String hlq = params.get(HighlightParams.Q);
      String hlparser =
          Objects.requireNonNullElse(
              params.get(HighlightParams.QPARSER),
              params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE));
      if (hlq != null) {
        try {
          QParser parser = QParser.getParser(hlq, hlparser, rb.req);
          rb.setHighlightQuery(parser.getHighlightQuery());
        } catch (SyntaxError e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
      }
    }
  }

  @Override
  public void inform(SolrCore core) {
    List<PluginInfo> children = info.getChildren("highlighting");
    if (children.isEmpty()) {
      DefaultSolrHighlighter defHighlighter = new DefaultSolrHighlighter(core);
      defHighlighter.init(PluginInfo.EMPTY_INFO);
      solrConfigHighlighter = defHighlighter;
    } else {
      solrConfigHighlighter =
          core.createInitInstance(
              children.get(0), SolrHighlighter.class, null, DefaultSolrHighlighter.class.getName());
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {

    if (rb.doHighlights) {
      SolrQueryRequest req = rb.req;
      SolrParams params = req.getParams();

      SolrHighlighter highlighter = getHighlighter(params);

      // TODO: get from builder by default?
      String[] defaultHighlightFields =
          rb.getQparser() != null ? rb.getQparser().getDefaultHighlightFields() : null;

      Query highlightQuery = rb.getHighlightQuery();
      if (highlightQuery == null) {
        if (rb.getQparser() != null) {
          try {
            highlightQuery = rb.getQparser().getHighlightQuery();
            rb.setHighlightQuery(highlightQuery);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
        } else {
          highlightQuery = rb.getQuery();
          rb.setHighlightQuery(highlightQuery);
        }
      }

      // No highlighting if there is no query -- consider q.alt=*:*
      if (highlightQuery != null) {
        NamedList<Object> sumData =
            highlighter.doHighlighting(
                rb.getResults().docList, highlightQuery, req, defaultHighlightFields);

        if (sumData != null) {
          // TODO ???? add this directly to the response?
          rb.rsp.add(highlightingResponseField(), convertHighlights(sumData));
        }
      }
    }
  }

  /** The highlighter given the param {@link HighlightParams#METHOD}. Never returns null. */
  public SolrHighlighter getHighlighter(SolrParams params) {
    HighlightMethod method = HighlightMethod.parse(params.get(HighlightParams.METHOD, "unified"));

    switch (method) {
      case UNIFIED:
        if (solrConfigHighlighter instanceof UnifiedSolrHighlighter) {
          return solrConfigHighlighter;
        }
        return new UnifiedSolrHighlighter(); // cheap
      case FAST_VECTOR: // fall-through
      case ORIGINAL:
        // The configured highlighter might not actually be the original highlighter if
        //  someone specified class= something custom.
        // Perhaps we shouldn't even support custom SolrHighlighter impls, and instead
        //  we ask users to subclass HighlightComponent instead?
        return solrConfigHighlighter;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doHighlights) return;

    // Turn on highlighting only only when retrieving fields
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_HIGHLIGHTS;
      // should already be true...
      sreq.params.set(HighlightParams.HIGHLIGHT, "true");
    } else {
      sreq.params.set(HighlightParams.HIGHLIGHT, "false");
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {}

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.doHighlights && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {

      final Object[] objArr = newHighlightsArray(rb.resultIds.size());
      final String highlightingResponseField = highlightingResponseField();

      // TODO: make a generic routine to do automatic merging of id keyed data
      for (ShardRequest sreq : rb.finished) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_HIGHLIGHTS) == 0) continue;
        for (ShardResponse srsp : sreq.responses) {
          if (srsp.getException() != null) {
            // can't expect the highlight content if there was an exception for this request
            // this should only happen when using shards.tolerant=true
            continue;
          }
          Object hl =
              SolrResponseUtil.getSubsectionFromShardResponse(
                  rb, srsp, highlightingResponseField, false);
          if (hl == null) {
            continue;
          }
          addHighlights(objArr, hl, rb.resultIds);
        }
      }

      rb.rsp.add(highlightingResponseField, getAllHighlights(objArr));
    }
  }

  ////////////////////////////////////////////
  ///  SolrInfoBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Highlighting";
  }

  @Override
  public Category getCategory() {
    return Category.HIGHLIGHTER;
  }

  ////////////////////////////////////////////
  ///  highlighting response collation
  ////////////////////////////////////////////

  protected String highlightingResponseField() {
    return "highlighting";
  }

  protected Object convertHighlights(NamedList<Object> hl) {
    return hl;
  }

  protected Object[] newHighlightsArray(int size) {
    // Curious why this doesn't trigger an unchecked cast, but maybe the compiler is smart enough to
    // know
    return (Object[]) Array.newInstance(NamedList.NamedListEntry.class, size);
  }

  protected void addHighlights(Object[] objArr, Object obj, Map<Object, ShardDoc> resultIds) {
    @SuppressWarnings({"unchecked"})
    Map.Entry<String, Object>[] arr = (Map.Entry<String, Object>[]) objArr;
    @SuppressWarnings("unchecked")
    NamedList<Object> hl = (NamedList<Object>) obj;
    SolrPluginUtils.copyNamedListIntoArrayByDocPosInResponse(hl, resultIds, arr);
  }

  protected Object getAllHighlights(Object[] objArr) {
    @SuppressWarnings({"unchecked"})
    final Map.Entry<String, Object>[] arr = (Map.Entry<String, Object>[]) objArr;
    // remove nulls in case not all docs were able to be retrieved
    return SolrPluginUtils.removeNulls(arr, new SimpleOrderedMap<>());
  }
}
