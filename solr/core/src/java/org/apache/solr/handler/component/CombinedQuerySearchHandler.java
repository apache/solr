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
import org.apache.solr.search.facet.FacetModule;

/**
 * The CombinedQuerySearchHandler class extends the SearchHandler and provides custom behavior for
 * handling combined queries. It overrides methods to create a response builder based on the {@link
 * CombinerParams#COMBINER} parameter and to define the default components included in the search
 * configuration.
 */
public class CombinedQuerySearchHandler extends SearchHandler {

  /**
   * Overrides the default response builder creation method. This method checks if the {@link
   * CombinerParams#COMBINER} parameter is set to true in the request. If it is, it returns an
   * instance of {@link CombinedQueryResponseBuilder}, otherwise, it returns an instance of {@link
   * ResponseBuilder}.
   *
   * @param req the SolrQueryRequest object
   * @param rsp the SolrQueryResponse object
   * @param components the list of SearchComponent objects
   * @return the appropriate ResponseBuilder instance based on the CombinerParams.COMBINER parameter
   */
  @Override
  protected ResponseBuilder newResponseBuilder(
      SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    if (req.getParams().getBool(CombinerParams.COMBINER, false)) {
      return new CombinedQueryResponseBuilder(req, rsp, components);
    }
    return new ResponseBuilder(req, rsp, components);
  }

  /**
   * Overrides the default components and returns a list of component names that are included in the
   * default configuration.
   *
   * @return a list of component names
   */
  @Override
  @SuppressWarnings("unchecked")
  protected List<String> getDefaultComponents() {
    List<String> names = new ArrayList<>(9);
    names.add(CombinedQueryComponent.COMPONENT_NAME);
    names.add(FacetComponent.COMPONENT_NAME);
    names.add(FacetModule.COMPONENT_NAME);
    names.add(MoreLikeThisComponent.COMPONENT_NAME);
    names.add(HighlightComponent.COMPONENT_NAME);
    names.add(StatsComponent.COMPONENT_NAME);
    names.add(DebugComponent.COMPONENT_NAME);
    names.add(ExpandComponent.COMPONENT_NAME);
    names.add(TermsComponent.COMPONENT_NAME);
    return names;
  }
}
