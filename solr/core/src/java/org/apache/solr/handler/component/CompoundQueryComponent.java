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
import org.apache.solr.response.SolrQueryResponse;

public class CompoundQueryComponent extends QueryComponent {

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb instanceof CompoundResponseBuilder) {
      CompoundResponseBuilder crb = (CompoundResponseBuilder) rb;

      ResponseBuilder rb_1 = new ResponseBuilder(rb.req, new SolrQueryResponse(), rb.components);
      rb_1.setQueryString(rb_1.req.getParams().get("rrf.q.1"));
      crb.responseBuilders.add(rb_1);

      ResponseBuilder rb_2 = new ResponseBuilder(rb.req, new SolrQueryResponse(), rb.components);
      rb_2.setQueryString(rb_2.req.getParams().get("rrf.q.2"));
      crb.responseBuilders.add(rb_2);

      for (ResponseBuilder rb_i : crb.responseBuilders) {
        super.prepare(rb_i);
      }
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb instanceof CompoundResponseBuilder) {
      CompoundResponseBuilder crb = (CompoundResponseBuilder) rb;

      for (ResponseBuilder rb_i : crb.responseBuilders) {
        super.process(rb_i);
      }

      // TODO: combine crb.responseBuilders into rb
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    // TODO
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    // TODO
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb instanceof CompoundResponseBuilder) {
      CompoundResponseBuilder crb = (CompoundResponseBuilder) rb;

      for (ResponseBuilder rb_i : crb.responseBuilders) {
        super.finishStage(rb_i);
      }

      // TODO: combine crb.responseBuilders into rb
    }
  }

  @Override
  public String getDescription() {
    return "compoundQuery";
  }
}
