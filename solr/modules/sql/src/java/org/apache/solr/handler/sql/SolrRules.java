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
package org.apache.solr.handler.sql;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/** Rules and relational operators for {@link SolrRel#CONVENTION} calling convention. */
class SolrRules {
  static final RelOptRule[] RULES = {
    SolrSortRule.SORT_RULE,
    SolrFilterRule.FILTER_RULE,
    SolrProjectRule.PROJECT_RULE,
    SolrAggregateRule.AGGREGATE_RULE,
  };

  static List<String> solrFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<>() {
          @Override
          public String get(int index) {
            return rowType.getFieldList().get(index).getName();
          }

          @Override
          public int size() {
            return rowType.getFieldCount();
          }
        },
        true);
  }

  /** Translator from {@link RexNode} to strings in Solr's expression language. */
  static class RexToSolrTranslator extends RexVisitorImpl<String> {
    private final List<String> inFields;

    RexToSolrTranslator(List<String> inFields) {
      super(true);
      this.inFields = inFields;
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }

    @Override
    public String visitCall(RexCall call) {
      final List<String> strings = visitList(call.operands);
      if (call.getKind() == SqlKind.CAST) {
        return strings.get(0);
      }

      return super.visitCall(call);
    }

    private List<String> visitList(List<RexNode> list) {
      final List<String> strings = new ArrayList<>();
      for (RexNode node : list) {
        strings.add(node.accept(this));
      }
      return strings;
    }
  }

  /**
   * Base class for planner rules that convert a relational expression to Solr calling convention.
   */
  abstract static class SolrConverterRule extends ConverterRule {
    protected SolrConverterRule(ConverterRule.Config config) {
      super(config);
    }
  }

  /** Rule to convert a {@link LogicalFilter} to a {@link SolrFilter}. */
  private static class SolrFilterRule extends SolrConverterRule {
    private static boolean isNotFilterByExpr(List<RexNode> rexNodes, List<String> fieldNames) {

      // We don't have a way to filter by result of aggregator now
      boolean result = true;

      for (RexNode rexNode : rexNodes) {
        if (rexNode instanceof RexCall) {
          result = result && isNotFilterByExpr(((RexCall) rexNode).getOperands(), fieldNames);
        } else if (rexNode instanceof RexInputRef) {
          result =
              result && !fieldNames.get(((RexInputRef) rexNode).getIndex()).startsWith("EXPR$");
        }
      }
      return result;
    }

    private static boolean filter(RelNode relNode) {
      List<RexNode> filterOperands =
          ((RexCall) ((LogicalFilter) relNode).getCondition()).getOperands();
      return isNotFilterByExpr(filterOperands, SolrRules.solrFieldNames(relNode.getRowType()));
    }

    static final SolrFilterRule FILTER_RULE =
        new SolrFilterRule(
            ConverterRule.Config.INSTANCE
                .withConversion(
                    LogicalFilter.class,
                    SolrFilterRule::filter,
                    Convention.NONE,
                    SolrRel.CONVENTION,
                    "SolrFilterRule")
                .withRuleFactory(SolrFilterRule::new));

    private SolrFilterRule(ConverterRule.Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return filter(call.rel(0));
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new SolrFilter(
          rel.getCluster(), traitSet, convert(filter.getInput(), out), filter.getCondition());
    }
  }

  /** Rule to convert a {@link LogicalProject} to a {@link SolrProject}. */
  private static class SolrProjectRule extends SolrConverterRule {
    static final SolrProjectRule PROJECT_RULE =
        new SolrProjectRule(
            ConverterRule.Config.INSTANCE
                .withConversion(
                    LogicalProject.class,
                    SolrProjectRule::isSupported,
                    Convention.NONE,
                    SolrRel.CONVENTION,
                    "SolrProjectRule")
                .withRuleFactory(SolrProjectRule::new));

    private SolrProjectRule(ConverterRule.Config config) {
      super(config);
    }

    /**
     * Reject projects where any expression is a bare literal (possibly wrapped in CASTs). This
     * prevents SolrProject from being created for Calcite 1.42+ plans where constant-folding
     * replaces a grouped field with a literal constant (e.g. WHERE str_s='a' causes Calcite to
     * substitute str_s with the literal 'a' in a DISTINCT output project). Such projects must
     * remain as {@link LogicalProject} so Calcite's enumerable layer handles the substitution.
     */
    private static boolean isSupported(RelNode relNode) {
      for (RexNode expr : ((LogicalProject) relNode).getProjects()) {
        if (isLiteralExpr(expr)) return false;
      }
      return true;
    }

    /** Returns true if the expression is a literal, possibly wrapped in one or more CASTs. */
    private static boolean isLiteralExpr(RexNode node) {
      while (node instanceof RexCall && node.getKind() == SqlKind.CAST) {
        node = ((RexCall) node).getOperands().get(0);
      }
      return node instanceof RexLiteral;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return isSupported(call.rel(0));
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelNode converted = convert(project.getInput(), out);
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new SolrProject(
          rel.getCluster(), traitSet, converted, project.getProjects(), project.getRowType());
    }
  }

  /** Rule to convert a {@link LogicalSort} to a {@link SolrSort}. */
  private static class SolrSortRule extends SolrConverterRule {
    static final SolrSortRule SORT_RULE =
        new SolrSortRule(
            ConverterRule.Config.INSTANCE
                .withConversion(
                    LogicalSort.class, Convention.NONE, SolrRel.CONVENTION, "SolrSortRule")
                .withRuleFactory(SolrSortRule::new));

    private SolrSortRule(ConverterRule.Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
      return new SolrSort(
          rel.getCluster(),
          traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation(),
          sort.offset,
          sort.fetch);
    }
  }

  /** Rule to convert an {@link LogicalAggregate} to an {@link SolrAggregate}. */
  private static class SolrAggregateRule extends SolrConverterRule {
    static final SolrAggregateRule AGGREGATE_RULE =
        new SolrAggregateRule(
            ConverterRule.Config.INSTANCE
                .withConversion(
                    LogicalAggregate.class,
                    Convention.NONE,
                    SolrRel.CONVENTION,
                    "SolrAggregateRule")
                .withRuleFactory(SolrAggregateRule::new));

    private SolrAggregateRule(ConverterRule.Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      final RelTraitSet traitSet = agg.getTraitSet().replace(out);
      return new SolrAggregate(
          rel.getCluster(),
          traitSet,
          agg.getHints(),
          convert(agg.getInput(), traitSet.simplify()),
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    }
  }
}
