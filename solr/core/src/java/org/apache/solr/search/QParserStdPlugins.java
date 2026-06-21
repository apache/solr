package org.apache.solr.search;

import org.apache.solr.search.join.BlockJoinChildQParserPlugin;
import org.apache.solr.search.join.BlockJoinParentQParserPlugin;
import org.apache.solr.search.join.FiltersQParserPlugin;
import org.apache.solr.search.join.GraphQParserPlugin;
import org.apache.solr.search.join.HashRangeQParserPlugin;
import org.apache.solr.search.join.XCJFQParserPlugin;
import org.apache.solr.search.mlt.MLTQParserPlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QParserStdPlugins {
  /**
   * Internal use - name to parser for the builtin parsers.
   * Each query parser plugin extending {@link QParserPlugin} has own instance of standardPlugins.
   * This leads to cyclic dependencies of static fields and to case when NAME field is not yet initialized.
   * This result to NPE during initialization.
   * For every plugin, listed here, NAME field has to be final and static.
   */
  public static final Map<String, QParserPlugin> standardPlugins;

  static {
    HashMap<String, QParserPlugin> map = new HashMap<>(30, 1);
    map.put(LuceneQParserPlugin.NAME, new LuceneQParserPlugin());
    map.put(FunctionQParserPlugin.NAME, new FunctionQParserPlugin());
    map.put(PrefixQParserPlugin.NAME, new PrefixQParserPlugin());
    map.put(BoostQParserPlugin.NAME, new BoostQParserPlugin());
    map.put(DisMaxQParserPlugin.NAME, new DisMaxQParserPlugin());
    map.put(ExtendedDismaxQParserPlugin.NAME, new ExtendedDismaxQParserPlugin());
    map.put(FieldQParserPlugin.NAME, new FieldQParserPlugin());
    map.put(RawQParserPlugin.NAME, new RawQParserPlugin());
    map.put(TermQParserPlugin.NAME, new TermQParserPlugin());
    map.put(TermsQParserPlugin.NAME, new TermsQParserPlugin());
    map.put(NestedQParserPlugin.NAME, new NestedQParserPlugin());
    map.put(FunctionRangeQParserPlugin.NAME, new FunctionRangeQParserPlugin());
    map.put(SpatialFilterQParserPlugin.NAME, new SpatialFilterQParserPlugin());
    map.put(SpatialBoxQParserPlugin.NAME, new SpatialBoxQParserPlugin());
    map.put(JoinQParserPlugin.NAME, new JoinQParserPlugin());
    map.put(SurroundQParserPlugin.NAME, new SurroundQParserPlugin());
    map.put(SwitchQParserPlugin.NAME, new SwitchQParserPlugin());
    map.put(MaxScoreQParserPlugin.NAME, new MaxScoreQParserPlugin());
    map.put(BlockJoinParentQParserPlugin.NAME, new BlockJoinParentQParserPlugin());
    map.put(BlockJoinChildQParserPlugin.NAME, new BlockJoinChildQParserPlugin());
    map.put(FiltersQParserPlugin.NAME, new FiltersQParserPlugin());
    map.put(CollapsingQParserPlugin.NAME, new CollapsingQParserPlugin());
    map.put(SimpleQParserPlugin.NAME, new SimpleQParserPlugin());
    map.put(ComplexPhraseQParserPlugin.NAME, new ComplexPhraseQParserPlugin());
    map.put(ReRankQParserPlugin.NAME, new ReRankQParserPlugin());
    map.put(ExportQParserPlugin.NAME, new ExportQParserPlugin());
    map.put(MLTQParserPlugin.NAME, new MLTQParserPlugin());
    map.put(HashQParserPlugin.NAME, new HashQParserPlugin());
    map.put(GraphQParserPlugin.NAME, new GraphQParserPlugin());
    map.put(XmlQParserPlugin.NAME, new XmlQParserPlugin());
    map.put(GraphTermsQParserPlugin.NAME, new GraphTermsQParserPlugin());
    map.put(IGainTermsQParserPlugin.NAME, new IGainTermsQParserPlugin());
    map.put(TextLogisticRegressionQParserPlugin.NAME, new TextLogisticRegressionQParserPlugin());
    map.put(SignificantTermsQParserPlugin.NAME, new SignificantTermsQParserPlugin());
    map.put(PayloadScoreQParserPlugin.NAME, new PayloadScoreQParserPlugin());
    map.put(PayloadCheckQParserPlugin.NAME, new PayloadCheckQParserPlugin());
    map.put(BoolQParserPlugin.NAME, new BoolQParserPlugin());
    map.put(MinHashQParserPlugin.NAME, new MinHashQParserPlugin());
    map.put(XCJFQParserPlugin.NAME, new XCJFQParserPlugin());
    map.put(HashRangeQParserPlugin.NAME, new HashRangeQParserPlugin());

    standardPlugins = Collections.unmodifiableMap(map);
  }
}
