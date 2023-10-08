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

import static org.apache.solr.common.params.CommonParams.SORT;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.MoreLikeThisHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO!
 *
 * @since solr 1.3
 */
public class MoreLikeThisComponent extends SearchComponent {
  public static final String COMPONENT_NAME = "mlt";
  public static final String DIST_DOC_ID = "mlt.dist.id";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(MoreLikeThisParams.MLT, false)) {
      rb.setNeedDocList(true);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {

    SolrParams params = rb.req.getParams();

    if (params.getBool(MoreLikeThisParams.MLT, false)) {
      ReturnFields returnFields = new SolrReturnFields(rb.req);

      int flags = 0;
      if (returnFields.wantsScore()) {
        flags |= SolrIndexSearcher.GET_SCORES;
      }

      rb.setFieldFlags(flags);

      if (log.isDebugEnabled()) {
        log.debug(
            "Starting MoreLikeThis.Process.  isShard: {}", params.getBool(ShardParams.IS_SHARD));
      }
      SolrIndexSearcher searcher = rb.req.getSearcher();

      if (params.getBool(ShardParams.IS_SHARD, false)) {
        if (params.get(MoreLikeThisComponent.DIST_DOC_ID) == null) {
          if (rb.getResults().docList.size() == 0) {
            // return empty response
            rb.rsp.add("moreLikeThis", new NamedList<DocList>());
            return;
          }
          MoreLikeThisHandler.MoreLikeThisHelper mlt =
              new MoreLikeThisHandler.MoreLikeThisHelper(params, searcher);
          NamedList<NamedList<?>> mltQueryByDocKey = new NamedList<>();
          for (DocIterator results = rb.getResults().docList.iterator(); results.hasNext(); ) {
            int docId = results.nextDoc();
            final List<MoreLikeThisHandler.InterestingTerm> interestingTerms =
                mlt.getInterestingTerms(mlt.getBoostedMLTQuery(docId), -1);
            if (interestingTerms.isEmpty()) {
              continue;
            }
            final String uniqueKey = rb.req.getSchema().getUniqueKeyField().getName();
            final Document document = rb.req.getSearcher().doc(docId);
            final String uniqueVal = rb.req.getSchema().printableUniqueKey(document);
            final NamedList<String> mltQ =
                mltViaQueryParams(rb.req.getSchema(), interestingTerms, uniqueKey, uniqueVal);
            mltQueryByDocKey.add(uniqueVal, mltQ);
          }
          rb.rsp.add("moreLikeThis", mltQueryByDocKey);
        } else {
          NamedList<DocList> sim =
              getMoreLikeThese(rb, rb.req.getSearcher(), rb.getResults().docList, flags);
          rb.rsp.add("moreLikeThis", sim);
        }
      } else {
        // non distrib case
        NamedList<DocList> sim =
            getMoreLikeThese(rb, rb.req.getSearcher(), rb.getResults().docList, flags);
        rb.rsp.add("moreLikeThis", sim);
      }
    }
  }

  private static NamedList<String> mltViaQueryParams(
      IndexSchema schema,
      List<MoreLikeThisHandler.InterestingTerm> terms,
      String uniqueField,
      String uniqueVal) {
    final NamedList<String> mltQ = new NamedList<>();
    StringBuilder q = new StringBuilder("{!bool");
    q.append(" must_not=$");
    int cnt = 0;
    String param = "mltq" + (cnt++);
    q.append(param);
    mltQ.add(param, "{!field f=" + uniqueField + "}" + uniqueVal);
    final StringBuilder reuseStr = new StringBuilder();
    final CharsRefBuilder reuseChar = new CharsRefBuilder();
    for (MoreLikeThisHandler.InterestingTerm term : terms) {
      param = "mltq" + (cnt++);
      q.append(" should=$");
      q.append(param);
      mltQ.add(param, toParserParam(schema, term.term, term.boost, reuseStr, reuseChar));
    }
    q.append("}");
    mltQ.add(CommonParams.Q, q.toString());
    return mltQ;
  }

  private static String toParserParam(
      IndexSchema schema,
      Term term1,
      float boost,
      StringBuilder reuseStr,
      CharsRefBuilder reuseChar) {
    reuseStr.setLength(0);
    if (boost != 1f) {
      reuseStr.append("{!boost b=");
      reuseStr.append(boost);
      reuseStr.append("}");
    }
    final String field = term1.field();
    final CharsRef val =
        schema.getField(field).getType().indexedToReadable(term1.bytes(), reuseChar);
    reuseStr.append("{!term f=");
    reuseStr.append(ClientUtils.encodeLocalParamVal(field));
    reuseStr.append("}");
    reuseStr.append(val);
    return reuseStr.toString();
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0
        && rb.req.getParams().getBool(COMPONENT_NAME, false)) {
      if (log.isDebugEnabled()) {
        log.debug("ShardRequest.response.size: {}", sreq.responses.size());
      }
      for (ShardResponse r : sreq.responses) {
        if (r.getException() != null) {
          // This should only happen in case of using shards.tolerant=true. Omit this ShardResponse
          continue;
        }
        @SuppressWarnings("unchecked")
        NamedList<NamedList<String>> moreLikeThisReponse =
            (NamedList<NamedList<String>>) r.getSolrResponse().getResponse().get("moreLikeThis");
        if (log.isDebugEnabled()) {
          log.debug("ShardRequest.response.shard: {}", r.getShard());
        }
        if (moreLikeThisReponse != null) {
          for (Entry<String, NamedList<String>> entry : moreLikeThisReponse) {
            if (log.isDebugEnabled()) {
              log.debug("id: '{}' Query: '{}'", entry.getKey(), entry.getValue());
            }
            ShardRequest s = buildShardQuery(rb, entry.getValue(), entry.getKey());
            rb.addRequest(this, s);
          }
        }
      }
    }

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_MLT_RESULTS) != 0) {
      for (ShardResponse r : sreq.responses) {
        if (log.isDebugEnabled()) {
          log.debug("MLT Query returned: {}", r.getSolrResponse().getResponse());
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {

    // Handling Responses in finishStage, because solrResponse will put
    // moreLikeThis xml
    // segment ahead of result/response.
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS
        && rb.req.getParams().getBool(COMPONENT_NAME, false)) {
      Map<Object, SolrDocumentList> tempResults = new LinkedHashMap<>();

      int mltcount =
          rb.req
              .getParams()
              .getInt(MoreLikeThisParams.DOC_COUNT, MoreLikeThisParams.DEFAULT_DOC_COUNT);
      String keyName = rb.req.getSchema().getUniqueKeyField().getName();

      for (ShardRequest sreq : rb.finished) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_MLT_RESULTS) != 0) {
          for (ShardResponse r : sreq.responses) {
            if (log.isDebugEnabled()) {
              log.debug("ShardRequest.response.shard: {}", r.getShard());
            }
            String key = r.getShardRequest().params.get(MoreLikeThisComponent.DIST_DOC_ID);
            SolrDocumentList shardDocList =
                (SolrDocumentList) r.getSolrResponse().getResponse().get("response");

            if (shardDocList == null) {
              continue;
            }

            log.info("MLT: results added for key: {} documents: {}", key, shardDocList);
            SolrDocumentList mergedDocList = tempResults.get(key);

            if (mergedDocList == null) {
              mergedDocList = new SolrDocumentList();
              mergedDocList.addAll(shardDocList);
              mergedDocList.setNumFound(shardDocList.getNumFound());
              mergedDocList.setStart(shardDocList.getStart());
              mergedDocList.setMaxScore(shardDocList.getMaxScore());
            } else {
              mergedDocList = mergeSolrDocumentList(mergedDocList, shardDocList, mltcount, keyName);
            }
            log.debug("Adding docs for key: {}", key);
            tempResults.put(key, mergedDocList);
          }
        }
      }

      NamedList<SolrDocumentList> list = buildMoreLikeThisNamed(tempResults, rb.resultIds);

      rb.rsp.add("moreLikeThis", list);
    }
    super.finishStage(rb);
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) return;
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_MLT_RESULTS) == 0
        && (sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) == 0) {
      sreq.params.set(COMPONENT_NAME, "false");
    }
  }

  /** Returns NamedList based on the order of resultIds.shardDoc.positionInResponse */
  NamedList<SolrDocumentList> buildMoreLikeThisNamed(
      Map<Object, SolrDocumentList> allMlt, Map<Object, ShardDoc> resultIds) {
    NamedList<SolrDocumentList> result = new NamedList<>();
    TreeMap<Integer, Object> sortingMap = new TreeMap<>();
    for (Entry<Object, ShardDoc> next : resultIds.entrySet()) {
      sortingMap.put(next.getValue().positionInResponse, next.getKey());
    }
    for (Object key : sortingMap.values()) {
      SolrDocumentList sdl = allMlt.get(key);
      if (sdl == null) {
        sdl = new SolrDocumentList();
        sdl.setNumFound(0);
        sdl.setStart(0);
      }
      result.add(key.toString(), sdl);
    }
    return result;
  }

  public SolrDocumentList mergeSolrDocumentList(
      SolrDocumentList one, SolrDocumentList two, int maxSize, String idField) {

    List<SolrDocument> l = new ArrayList<>();

    // De-dup records sets. Shouldn't happen if indexed correctly.
    Map<String, SolrDocument> map = new HashMap<>();
    for (SolrDocument doc : one) {
      Object id = doc.getFieldValue(idField);
      assert id != null : doc.toString();
      map.put(id.toString(), doc);
    }
    for (SolrDocument doc : two) {
      map.put(doc.getFieldValue(idField).toString(), doc);
    }

    l = new ArrayList<>(map.values());

    // Comparator to sort docs based on score. null scores/docs are set to 0.

    // hmm...we are ordering by scores that are not really comparable...
    Comparator<SolrDocument> c =
        new Comparator<SolrDocument>() {
          @Override
          public int compare(SolrDocument o1, SolrDocument o2) {
            Float f1 = getFloat(o1);
            Float f2 = getFloat(o2);
            return f2.compareTo(f1);
          }

          private Float getFloat(SolrDocument doc) {
            Float f = 0f;
            if (doc != null) {
              Object o = doc.getFieldValue("score");
              if (o != null && o instanceof Float) {
                f = (Float) o;
              }
            }
            return f;
          }
        };

    l.sort(c);

    // Truncate list to maxSize
    if (l.size() > maxSize) {
      l = l.subList(0, maxSize);
    }

    // Create SolrDocumentList Attributes from originals
    SolrDocumentList result = new SolrDocumentList();
    result.addAll(l);
    result.setMaxScore(Math.max(one.getMaxScore(), two.getMaxScore()));
    result.setNumFound(one.getNumFound() + two.getNumFound());
    result.setStart(Math.min(one.getStart(), two.getStart()));

    return result;
  }

  ShardRequest buildShardQuery(ResponseBuilder rb, NamedList<String> q, String key) {
    ShardRequest s = new ShardRequest();
    s.params = new ModifiableSolrParams(rb.req.getParams());
    s.purpose |= ShardRequest.PURPOSE_GET_MLT_RESULTS;
    // Maybe unnecessary, but safe.
    s.purpose |= ShardRequest.PURPOSE_PRIVATE;

    s.params.remove(ShardParams.SHARDS);
    // s.params.remove(MoreLikeThisComponent.COMPONENT_NAME);

    // needed to correlate results
    s.params.set(MoreLikeThisComponent.DIST_DOC_ID, key);
    s.params.set(CommonParams.START, 0);
    int mltcount = s.params.getInt(MoreLikeThisParams.DOC_COUNT, 20); // overrequest
    s.params.set(CommonParams.ROWS, mltcount);

    // adding score to rank moreLikeThis
    s.params.remove(CommonParams.FL);

    // Should probably add something like this:
    // String fl = s.params.get(MoreLikeThisParams.RETURN_FL, "*");
    // if(fl != null){
    // s.params.set(CommonParams.FL, fl + ",score");
    // }
    String id = rb.req.getSchema().getUniqueKeyField().getName();
    s.params.set(CommonParams.FL, "score," + id);
    s.params.set(SORT, "score desc");
    // MLT Query is submitted as normal query to shards.
    s.params.remove(CommonParams.Q);
    q.forEach((k, v) -> s.params.add(k, v));

    return s;
  }

  NamedList<DocList> getMoreLikeThese(
      ResponseBuilder rb, SolrIndexSearcher searcher, DocList docs, int flags) throws IOException {
    SolrParams p = rb.req.getParams();
    IndexSchema schema = searcher.getSchema();
    MoreLikeThisHandler.MoreLikeThisHelper mltHelper =
        new MoreLikeThisHandler.MoreLikeThisHelper(p, searcher);
    NamedList<DocList> mltResponse = new SimpleOrderedMap<>();
    DocIterator iterator = docs.iterator();

    SimpleOrderedMap<Object> dbg = null;
    if (rb.isDebug()) {
      dbg = new SimpleOrderedMap<>();
    }

    SimpleOrderedMap<Object> interestingTermsResponse = null;
    MoreLikeThisParams.TermStyle interestingTermsConfig =
        MoreLikeThisParams.TermStyle.get(p.get(MoreLikeThisParams.INTERESTING_TERMS));

    if (interestingTermsConfig != MoreLikeThisParams.TermStyle.NONE) {
      interestingTermsResponse = new SimpleOrderedMap<>();
    }

    while (iterator.hasNext()) {
      int id = iterator.nextDoc();
      int rows = p.getInt(MoreLikeThisParams.DOC_COUNT, 5);

      DocListAndSet similarDocuments = mltHelper.getMoreLikeThis(id, 0, rows, null, flags);
      String name = schema.printableUniqueKey(searcher.doc(id));
      mltResponse.add(name, similarDocuments.docList);

      if (dbg != null) {
        SimpleOrderedMap<Object> docDbg = new SimpleOrderedMap<>();
        docDbg.add("rawMLTQuery", mltHelper.getRawMLTQuery().toString());
        docDbg.add("boostedMLTQuery", mltHelper.getBoostedMLTQuery().toString());
        docDbg.add("realMLTQuery", mltHelper.getRealMLTQuery().toString());
        SimpleOrderedMap<Object> explains = new SimpleOrderedMap<>();
        DocIterator similarDocumentsIterator = similarDocuments.docList.iterator();
        while (similarDocumentsIterator.hasNext()) {
          int mltid = similarDocumentsIterator.nextDoc();
          String key = schema.printableUniqueKey(searcher.doc(mltid));
          explains.add(key, searcher.explain(mltHelper.getRealMLTQuery(), mltid));
        }
        docDbg.add("explain", explains);
        dbg.add(name, docDbg);
      }

      if (interestingTermsResponse != null) {
        List<MoreLikeThisHandler.InterestingTerm> interestingTerms =
            mltHelper.getInterestingTerms(
                mltHelper.getBoostedMLTQuery(), mltHelper.getMoreLikeThis().getMaxQueryTerms());
        if (interestingTermsConfig == MoreLikeThisParams.TermStyle.DETAILS) {
          SimpleOrderedMap<Float> interestingTermsWithScore = new SimpleOrderedMap<>();
          for (MoreLikeThisHandler.InterestingTerm interestingTerm : interestingTerms) {
            interestingTermsWithScore.add(interestingTerm.term.toString(), interestingTerm.boost);
          }
          interestingTermsResponse.add(name, interestingTermsWithScore);
        } else {
          List<String> interestingTermsString = new ArrayList<>(interestingTerms.size());
          for (MoreLikeThisHandler.InterestingTerm interestingTerm : interestingTerms) {
            interestingTermsString.add(interestingTerm.term.toString());
          }
          interestingTermsResponse.add(name, interestingTermsString);
        }
      }
    }
    // add debug information
    if (dbg != null) {
      rb.addDebugInfo("moreLikeThis", dbg);
    }
    // add Interesting Terms
    if (interestingTermsResponse != null) {
      rb.rsp.add("interestingTerms", interestingTermsResponse);
    }
    return mltResponse;
  }

  // ///////////////////////////////////////////
  // / SolrInfoBean
  // //////////////////////////////////////////

  @Override
  public String getDescription() {
    return "More Like This";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }
}
