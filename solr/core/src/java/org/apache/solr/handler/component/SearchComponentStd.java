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

import org.apache.solr.search.facet.FacetModule;

import java.util.Map;

// referencing subclasses from a class initializer can deadlock, so this is external to SearchComponent
public class SearchComponentStd {
  public static final Map<String, Class<? extends SearchComponent>> standard_components;

  static {

    standard_components = Map
        .of(HighlightComponent.COMPONENT_NAME, HighlightComponent.class, QueryComponent.COMPONENT_NAME, QueryComponent.class, FacetComponent.COMPONENT_NAME,
            FacetComponent.class, FacetModule.COMPONENT_NAME, FacetModule.class, MoreLikeThisComponent.COMPONENT_NAME, MoreLikeThisComponent.class,
            StatsComponent.COMPONENT_NAME, StatsComponent.class, DebugComponent.COMPONENT_NAME, DebugComponent.class, RealTimeGetComponent.COMPONENT_NAME,
            RealTimeGetComponent.class, ExpandComponent.COMPONENT_NAME, ExpandComponent.class, TermsComponent.COMPONENT_NAME, TermsComponent.class);
  }

}
