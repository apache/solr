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

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class CompoundResponseBuilder extends ResponseBuilder {

  public static final int STAGE_FUSION = STAGE_GET_FIELDS + 1;

  public static final String RRF_Q_KEY = "rrf.q.key";

  public static class Inner extends ResponseBuilder {
    private final CompoundResponseBuilder owner;
    private final String my_q_key;

    public Inner(CompoundResponseBuilder owner, String my_q_key) {
      super(owner.req, new SolrQueryResponse(), owner.components);
      this.owner = owner;
      this.my_q_key = my_q_key;
    }

    @Override
    protected String getQParameterName() {
      return my_q_key;
    }

    @Override
    protected int getDoneStage() {
      return STAGE_FUSION;
    }

    @Override
    public int getStage() {
      return owner.getStage();
    }

    @Override
    public void addRequest(SearchComponent me, ShardRequest sreq) {
      // send what CompoundQueryComponent uses to detect shard requests and also make it something that we can use to detect only our request/response
      sreq.params.set(RRF_Q_KEY, my_q_key);
      owner.addRequest(me, sreq);
    }

    public boolean isThisFromMe(ShardRequest sreq) {
      return my_q_key.equals(sreq.params.get(RRF_Q_KEY)); // detect our request/response
    }
  }
  ;

  public final List<Inner> responseBuilders = new ArrayList<>();

  public CompoundResponseBuilder(
      SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    super(req, rsp, components);
  }

  @Override
  protected String getQParameterName() {
    return req.getParams().get(RRF_Q_KEY, super.getQParameterName());
  }
}
