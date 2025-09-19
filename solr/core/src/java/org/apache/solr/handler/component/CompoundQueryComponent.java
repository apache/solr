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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;

public class CompoundQueryComponent extends QueryComponent {
  public static final String COMPONENT_NAME = "compound_query";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb instanceof CompoundResponseBuilder crb) {
      final var params = rb.req.getParams();
      if (params.get(CompoundResponseBuilder.RRF_PREFIX) == null) {
        for (var prefix : params.get(CompoundResponseBuilder.RRF_PREFIX + ".list").split(",")) {
          crb.responseBuilders.add(new CompoundResponseBuilder.Inner(crb, prefix));
        }
        for (var rb_i : crb.responseBuilders) {
          super.prepare(rb_i);
        }
      } else {
        super.prepare(rb);
      }
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    int nextStage = ResponseBuilder.STAGE_DONE;
    if (rb instanceof CompoundResponseBuilder crb) {
      if (rb.getStage() < CompoundResponseBuilder.STAGE_FUSION) {
        for (var rb_i : crb.responseBuilders) {
          nextStage = Math.min(nextStage, super.distributedProcess(rb_i));
        }
      } else if (rb.getStage() == CompoundResponseBuilder.STAGE_FUSION) {
        nextStage = doFusion(crb);
      }
    }
    return nextStage;
  }

  private int doFusion(CompoundResponseBuilder crb) {
    final SolrDocumentList responseDocs = new SolrDocumentList();
    final Map<Object, ShardDoc> resultIds = new HashMap<>();

    final TopDocs[] hits = new TopDocs[crb.responseBuilders.size()];
    for (int crb_idx = 0; crb_idx < crb.responseBuilders.size(); ++crb_idx) {

      final SolrDocumentList sdl = crb.responseBuilders.get(crb_idx).getResponseDocs();

      final ScoreDoc[] scoreDocs = new ScoreDoc[sdl.size()];
      for (int idx = 0; idx < sdl.size(); ++idx) {
        scoreDocs[idx] = new ScoreDoc(idx /* doc */, 0f /* score */, crb_idx /* shardIndex */);
      }

      final TotalHits totalHits =
          new TotalHits(
              sdl.getNumFound(),
              sdl.getNumFoundExact()
                  ? TotalHits.Relation.EQUAL_TO
                  : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);

      hits[crb_idx] = new TopDocs(totalHits, scoreDocs);
    }

    final var params = crb.req.getParams();
    final int topN = params.getInt(CommonParams.ROWS, CommonParams.ROWS_DEFAULT);
    final int k = params.getInt("rrf.k", 1);

    final TopDocs fusion = TopDocs.rrf(topN, k, hits);

    for (ScoreDoc scoreDoc : fusion.scoreDocs) {
      final ResponseBuilder crb_i = crb.responseBuilders.get(scoreDoc.shardIndex);
      final SolrDocument solrDocument = crb_i.getResponseDocs().get(scoreDoc.doc);
      final Object id = solrDocument.getFieldValue("id"); // TODO: do not hard-code "id" here
      final ShardDoc sdoc = crb_i.resultIds.get(id);
      sdoc.positionInResponse = resultIds.size();
      if (resultIds.putIfAbsent(id, sdoc) == null) {
        responseDocs.add(solrDocument);
      }
    }
    final TotalHits totalHits = fusion.totalHits;
    responseDocs.setNumFound(totalHits.value());
    responseDocs.setNumFoundExact(TotalHits.Relation.EQUAL_TO.equals(totalHits.relation()));

    crb.resultIds = resultIds;
    crb.setResponseDocs(responseDocs);

    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (rb instanceof CompoundResponseBuilder crb) {
      for (var rb_i : crb.responseBuilders) {
        if (rb_i.isThisFromMe(sreq)) {
          super.handleResponses(rb_i, sreq);
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb instanceof CompoundResponseBuilder crb) {
      for (var rb_i : crb.responseBuilders) {
        super.finishStage(rb_i);
      }

      if (rb.getStage() == CompoundResponseBuilder.STAGE_FUSION) {
        rb.rsp.addResponse(rb.getResponseDocs());
      }
    }
  }

  @Override
  public String getDescription() {
    return "compoundQuery";
  }
}
