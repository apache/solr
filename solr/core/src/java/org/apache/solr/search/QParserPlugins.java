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

import java.util.Map;
import org.apache.solr.search.join.BlockJoinChildQParserPlugin;
import org.apache.solr.search.join.BlockJoinParentQParserPlugin;
import org.apache.solr.search.join.FiltersQParserPlugin;
import org.apache.solr.search.join.GraphQParserPlugin;
import org.apache.solr.search.join.HashRangeQParserPlugin;
import org.apache.solr.search.mlt.MLTContentQParserPlugin;
import org.apache.solr.search.mlt.MLTQParserPlugin;
import org.apache.solr.search.neural.KnnQParserPlugin;
import org.apache.solr.search.neural.VectorSimilarityQParserPlugin;

public class QParserPlugins {

  private QParserPlugins() {}

  /**
   * Internal use - name to parser for the builtin parsers. Each query parser plugin extending
   * {@link QParserPlugin} has own instance of standardPlugins. This leads to cyclic dependencies of
   * static fields and to case when NAME field is not yet initialized. This result to NPE during
   * initialization. For every plugin, listed here, NAME field has to be final and static.
   */
  public static final Map<String, QParserPlugin> standardPlugins =
      Map.ofEntries(
          Map.entry(LuceneQParserPlugin.NAME, new LuceneQParserPlugin()),
          Map.entry(FunctionQParserPlugin.NAME, new FunctionQParserPlugin()),
          Map.entry(PrefixQParserPlugin.NAME, new PrefixQParserPlugin()),
          Map.entry(BoostQParserPlugin.NAME, new BoostQParserPlugin()),
          Map.entry(DisMaxQParserPlugin.NAME, new DisMaxQParserPlugin()),
          Map.entry(ExtendedDismaxQParserPlugin.NAME, new ExtendedDismaxQParserPlugin()),
          Map.entry(FieldQParserPlugin.NAME, new FieldQParserPlugin()),
          Map.entry(RawQParserPlugin.NAME, new RawQParserPlugin()),
          Map.entry(TermQParserPlugin.NAME, new TermQParserPlugin()),
          Map.entry(TermsQParserPlugin.NAME, new TermsQParserPlugin()),
          Map.entry(NestedQParserPlugin.NAME, new NestedQParserPlugin()),
          Map.entry(FunctionRangeQParserPlugin.NAME, new FunctionRangeQParserPlugin()),
          Map.entry(SpatialFilterQParserPlugin.NAME, new SpatialFilterQParserPlugin()),
          Map.entry(SpatialBoxQParserPlugin.NAME, new SpatialBoxQParserPlugin()),
          Map.entry(JoinQParserPlugin.NAME, new JoinQParserPlugin()),
          Map.entry(SurroundQParserPlugin.NAME, new SurroundQParserPlugin()),
          Map.entry(SwitchQParserPlugin.NAME, new SwitchQParserPlugin()),
          Map.entry(MaxScoreQParserPlugin.NAME, new MaxScoreQParserPlugin()),
          Map.entry(BlockJoinParentQParserPlugin.NAME, new BlockJoinParentQParserPlugin()),
          Map.entry(BlockJoinChildQParserPlugin.NAME, new BlockJoinChildQParserPlugin()),
          Map.entry(FiltersQParserPlugin.NAME, new FiltersQParserPlugin()),
          Map.entry(CollapsingQParserPlugin.NAME, new CollapsingQParserPlugin()),
          Map.entry(SimpleQParserPlugin.NAME, new SimpleQParserPlugin()),
          Map.entry(ComplexPhraseQParserPlugin.NAME, new ComplexPhraseQParserPlugin()),
          Map.entry(ReRankQParserPlugin.NAME, new ReRankQParserPlugin()),
          Map.entry(ExportQParserPlugin.NAME, new ExportQParserPlugin()),
          Map.entry(MLTQParserPlugin.NAME, new MLTQParserPlugin()),
          Map.entry(MLTContentQParserPlugin.NAME, new MLTContentQParserPlugin()),
          Map.entry(HashQParserPlugin.NAME, new HashQParserPlugin()),
          Map.entry(GraphQParserPlugin.NAME, new GraphQParserPlugin()),
          Map.entry(XmlQParserPlugin.NAME, new XmlQParserPlugin()),
          Map.entry(GraphTermsQParserPlugin.NAME, new GraphTermsQParserPlugin()),
          Map.entry(IGainTermsQParserPlugin.NAME, new IGainTermsQParserPlugin()),
          Map.entry(
              TextLogisticRegressionQParserPlugin.NAME, new TextLogisticRegressionQParserPlugin()),
          Map.entry(SignificantTermsQParserPlugin.NAME, new SignificantTermsQParserPlugin()),
          Map.entry(PayloadScoreQParserPlugin.NAME, new PayloadScoreQParserPlugin()),
          Map.entry(PayloadCheckQParserPlugin.NAME, new PayloadCheckQParserPlugin()),
          Map.entry(BoolQParserPlugin.NAME, new BoolQParserPlugin()),
          Map.entry(MinHashQParserPlugin.NAME, new MinHashQParserPlugin()),
          Map.entry(HashRangeQParserPlugin.NAME, new HashRangeQParserPlugin()),
          Map.entry(RankQParserPlugin.NAME, new RankQParserPlugin()),
          Map.entry(KnnQParserPlugin.NAME, new KnnQParserPlugin()),
          Map.entry(VectorSimilarityQParserPlugin.NAME, new VectorSimilarityQParserPlugin()));
}
