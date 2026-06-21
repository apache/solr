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
package org.apache.solr.search;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class QParserPlugin implements NamedListInitializedPlugin, SolrInfoBean {
  /** internal use - name of the default parser */
  public static final String DEFAULT_QTYPE = "lucene"; // subclass references can deadlock, so using string

  /** return a {@link QParser} */
  public abstract QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req);

  @Override
  public void init( @SuppressWarnings({"rawtypes"})NamedList args ) {
  }

  @Override
  public String getName() {
    // TODO: ideally use the NAME property that each qparser plugin has

    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "";  // UI required non-null to work
  }

  @Override
  public Category getCategory() {
    return Category.QUERYPARSER;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    // by default do nothing
  }

  // by default no metrics
  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return null;
  }
}


