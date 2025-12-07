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
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Extends the SearchHandler combining/fusing multiple queries (e.g. RRF) when the {@link
 * CombinerParams#COMBINER} param is provided. If it isn't, does nothing special over SearchHandler.
 *
 * @see CombinedQueryComponent
 */
public class CombinedQuerySearchHandler extends SearchHandler {

  /** Overrides to potentially return a custom {@link CombinedQueryResponseBuilder}. */
  @Override
  protected ResponseBuilder newResponseBuilder(
      SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    if (req.getParams().getBool(CombinerParams.COMBINER, false)) {
      var rb = new CombinedQueryResponseBuilder(req, rsp, components);
      // CombinedQueryComponent is only designed to work with distributed search.
      rb.setForcedDistrib(true);
      return rb;
    }
    return super.newResponseBuilder(req, rsp, components);
  }

  @Override
  protected void postPrepareComponents(ResponseBuilder rb) {
    super.postPrepareComponents(rb);
    // propagate the CombinedQueryResponseBuilder's state to all subBuilders after prepare
    if (rb instanceof CombinedQueryResponseBuilder crb) {
      crb.propagate();
    }
  }

  /** Overrides the default list to include {@link CombinedQueryComponent}. */
  @Override
  protected List<String> getDefaultComponents() {
    List<String> names = new ArrayList<>(super.getDefaultComponents());
    String replaced = names.set(0, CombinedQueryComponent.COMPONENT_NAME);
    assert replaced.equals(QueryComponent.COMPONENT_NAME);
    return names;
  }
}
