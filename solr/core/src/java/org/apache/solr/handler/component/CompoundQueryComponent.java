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
import org.apache.solr.common.SolrDocumentList;

public class CompoundQueryComponent extends QueryComponent {
  public static final String COMPONENT_NAME = "compound_query";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb instanceof CompoundResponseBuilder crb) {
      if (rb.req.getParams().get(CompoundResponseBuilder.RRF_Q_KEY) == null) {
        crb.responseBuilders.add(new CompoundResponseBuilder.Inner(crb, "rrf.q.1"));
        crb.responseBuilders.add(new CompoundResponseBuilder.Inner(crb, "rrf.q.2"));
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
    long numFound = 0;
    for (var rb_i : crb.responseBuilders) {
      responseDocs.addAll(rb_i.getResponseDocs());
      numFound += rb_i.getResponseDocs().getNumFound();
    }
    responseDocs.setNumFound(numFound);
    responseDocs.setNumFoundExact(false);
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
