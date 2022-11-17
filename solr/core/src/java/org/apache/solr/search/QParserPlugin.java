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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.join.BlockJoinChildQParserPlugin;
import org.apache.solr.search.join.BlockJoinParentQParserPlugin;
import org.apache.solr.search.join.FiltersQParserPlugin;
import org.apache.solr.search.join.GraphQParserPlugin;
import org.apache.solr.search.join.HashRangeQParserPlugin;
import org.apache.solr.search.mlt.MLTQParserPlugin;
import org.apache.solr.search.neural.KnnQParserPlugin;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class QParserPlugin implements NamedListInitializedPlugin, SolrInfoBean {
  private static final SolrConfig.SolrPluginInfo info =
      SolrConfig.classVsSolrPluginInfo.get(QParserPlugin.class.getName());
  /** internal use - name of the default parser */
  public static final String DEFAULT_QTYPE = LuceneQParserPlugin.NAME;

  public static PluginBag<QParserPlugin> create(SolrCore core) {
    PluginBag<QParserPlugin> pluginBag =
        new PluginBag<>(QParserPlugin.class, core, false, standardPlugins, info);

    pluginBag.init(null, core);
    return pluginBag;
  }

  public static QParserPlugin get(String name) {
    PluginBag.PluginHolder<QParserPlugin> result = standardPlugins.get(name);
    return result == null ? null : result.get();
  }

  /**
   * Internal use - name to parser for the builtin parsers. Each query parser plugin extending
   * {@link QParserPlugin} has own instance of standardPlugins. This leads to cyclic dependencies of
   * static fields and to case when NAME field is not yet initialized. This result to NPE during
   * initialization. For every plugin, listed here, NAME field has to be final and static.
   */
  public static final Map<String, PluginBag.PluginHolder<QParserPlugin>> standardPlugins;

  static {
    ImmutableMap.Builder<String, PluginBag.PluginHolder<QParserPlugin>> map =
        ImmutableMap.builder();

    map.put(LuceneQParserPlugin.NAME, wrap(new LuceneQParserPlugin()));
    map.put(FunctionQParserPlugin.NAME, wrap(new FunctionQParserPlugin()));
    map.put(PrefixQParserPlugin.NAME, wrap(new PrefixQParserPlugin()));
    map.put(BoostQParserPlugin.NAME, wrap(new BoostQParserPlugin()));
    map.put(DisMaxQParserPlugin.NAME, wrap(new DisMaxQParserPlugin()));
    map.put(ExtendedDismaxQParserPlugin.NAME, wrap(new ExtendedDismaxQParserPlugin()));
    map.put(FieldQParserPlugin.NAME, wrap(new FieldQParserPlugin()));
    map.put(RawQParserPlugin.NAME, wrap(new RawQParserPlugin()));
    map.put(TermQParserPlugin.NAME, wrap(new TermQParserPlugin()));
    map.put(TermsQParserPlugin.NAME, wrap(new TermsQParserPlugin()));
    map.put(NestedQParserPlugin.NAME, wrap(new NestedQParserPlugin()));
    map.put(FunctionRangeQParserPlugin.NAME, wrap(new FunctionRangeQParserPlugin()));
    map.put(SpatialFilterQParserPlugin.NAME, wrap(new SpatialFilterQParserPlugin()));
    map.put(SpatialBoxQParserPlugin.NAME, wrap(new SpatialBoxQParserPlugin()));
    map.put(JoinQParserPlugin.NAME, wrap(new JoinQParserPlugin()));
    map.put(SurroundQParserPlugin.NAME, wrap(new SurroundQParserPlugin()));
    map.put(SwitchQParserPlugin.NAME, wrap(new SwitchQParserPlugin()));
    map.put(MaxScoreQParserPlugin.NAME, wrap(new MaxScoreQParserPlugin()));
    map.put(BlockJoinParentQParserPlugin.NAME, wrap(new BlockJoinParentQParserPlugin()));
    map.put(BlockJoinChildQParserPlugin.NAME, wrap(new BlockJoinChildQParserPlugin()));
    map.put(FiltersQParserPlugin.NAME, wrap(new FiltersQParserPlugin()));
    map.put(CollapsingQParserPlugin.NAME, wrap(new CollapsingQParserPlugin()));
    map.put(SimpleQParserPlugin.NAME, wrap(new SimpleQParserPlugin()));
    map.put(ComplexPhraseQParserPlugin.NAME, wrap(new ComplexPhraseQParserPlugin()));
    map.put(ReRankQParserPlugin.NAME, wrap(new ReRankQParserPlugin()));
    map.put(ExportQParserPlugin.NAME, wrap(new ExportQParserPlugin()));
    map.put(MLTQParserPlugin.NAME, wrap(new MLTQParserPlugin()));
    map.put(HashQParserPlugin.NAME, wrap(new HashQParserPlugin()));
    map.put(GraphQParserPlugin.NAME, wrap(new GraphQParserPlugin()));
    map.put(XmlQParserPlugin.NAME, wrap(new XmlQParserPlugin()));
    map.put(GraphTermsQParserPlugin.NAME, wrap(new GraphTermsQParserPlugin()));
    map.put(IGainTermsQParserPlugin.NAME, wrap(new IGainTermsQParserPlugin()));
    map.put(
        TextLogisticRegressionQParserPlugin.NAME, wrap(new TextLogisticRegressionQParserPlugin()));
    map.put(SignificantTermsQParserPlugin.NAME, wrap(new SignificantTermsQParserPlugin()));
    map.put(PayloadScoreQParserPlugin.NAME, wrap(new PayloadScoreQParserPlugin()));
    map.put(PayloadCheckQParserPlugin.NAME, wrap(new PayloadCheckQParserPlugin()));
    map.put(BoolQParserPlugin.NAME, wrap(new BoolQParserPlugin()));
    map.put(MinHashQParserPlugin.NAME, wrap(new MinHashQParserPlugin()));
    map.put(HashRangeQParserPlugin.NAME, wrap(new HashRangeQParserPlugin()));
    map.put(RankQParserPlugin.NAME, wrap(new RankQParserPlugin()));
    map.put(KnnQParserPlugin.NAME, wrap(new KnnQParserPlugin()));
    standardPlugins = map.build();
  }

  private static PluginBag.PluginHolder<QParserPlugin> wrap(QParserPlugin v) {
    return new PluginBag.PluginHolder<>(v, info);
  }

  /** return a {@link QParser} */
  public abstract QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req);

  @Override
  public String getName() {
    // TODO: ideally use the NAME property that each qparser plugin has

    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return ""; // UI required non-null to work
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
