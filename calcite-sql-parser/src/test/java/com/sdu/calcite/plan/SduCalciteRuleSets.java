package com.sdu.calcite.plan;

import com.sdu.calcite.plan.rules.EnumerableToLogicalTableScan;
import com.sdu.calcite.plan.rules.LogicalCorrelateToTemporalTableJoinRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

class SduCalciteRuleSets {


  /**
   * Convert sub-queries before query decorrelation.
   * */
  static RuleSet TABLE_SUBQUERY_RULES = RuleSets.ofList(
      SubQueryRemoveRule.FILTER,
      SubQueryRemoveRule.PROJECT,
      SubQueryRemoveRule.JOIN);

  /**
   * Expand plan by replacing references to tables into a proper plan sub trees. Those rules
   * can create new plan nodes.
   */
  static RuleSet EXPAND_PLAN_RULES = RuleSets.ofList(
      LogicalCorrelateToTemporalTableJoinRule.INSTANCE,
      TableScanRule.INSTANCE);

  static RuleSet POST_EXPAND_CLEAN_UP_RULES = RuleSets.ofList(
    EnumerableToLogicalTableScan.INSTANCE);

  static RuleSet SDU_NORM_RULES = RuleSets.ofList(
      ReduceExpressionsRule.FILTER_INSTANCE,
      ReduceExpressionsRule.PROJECT_INSTANCE,
      ReduceExpressionsRule.CALC_INSTANCE,
      // ROW_NUMBER OVER构建LogicalWindow
      ProjectToWindowRule.PROJECT
  );

  static RuleSet LOGICAL_OPT_RULES = RuleSets.ofList(
      // push a filter into a join
      FilterJoinRule.FILTER_ON_JOIN,
      // push filter into the children of a join
      FilterJoinRule.JOIN,
      // push filter through an aggregation
      FilterAggregateTransposeRule.INSTANCE,
      // push filter through set operation
      FilterSetOpTransposeRule.INSTANCE,
      // push project through set operation
      ProjectSetOpTransposeRule.INSTANCE,

      // aggregation and projection rules
      AggregateProjectMergeRule.INSTANCE,
      AggregateProjectPullUpConstantsRule.INSTANCE,
      // push a projection past a filter or vice versa
      ProjectFilterTransposeRule.INSTANCE,
      FilterProjectTransposeRule.INSTANCE,
      // push a projection to the children of a join
      // push all expressions to handle the time indicator correctly
      new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
      // merge projections
      ProjectMergeRule.INSTANCE,
      // remove identity project
      ProjectRemoveRule.INSTANCE,
      // reorder sort and projection
      SortProjectTransposeRule.INSTANCE,
      ProjectSortTransposeRule.INSTANCE,

      // join rules
      JoinPushExpressionsRule.INSTANCE,

      // remove union with only a single child
      UnionEliminatorRule.INSTANCE,
      // convert non-all union into all-union + distinct
      UnionToDistinctRule.INSTANCE,

      // remove aggregation if it does not aggregate and input is already distinct
      AggregateRemoveRule.INSTANCE,
      // push aggregate through join
      AggregateJoinTransposeRule.EXTENDED,
      // aggregate union rule
      AggregateUnionAggregateRule.INSTANCE,

      // reduce aggregate functions like AVG, STDDEV_POP etc.
      AggregateReduceFunctionsRule.INSTANCE,

      // remove unnecessary sort rule
      SortRemoveRule.INSTANCE,

      // prune empty results rules
      PruneEmptyRules.AGGREGATE_INSTANCE,
      PruneEmptyRules.FILTER_INSTANCE,
      PruneEmptyRules.JOIN_LEFT_INSTANCE,
      PruneEmptyRules.JOIN_RIGHT_INSTANCE,
      PruneEmptyRules.PROJECT_INSTANCE,
      PruneEmptyRules.SORT_INSTANCE,
      PruneEmptyRules.UNION_INSTANCE,

      // calc rules
      FilterCalcMergeRule.INSTANCE,
      ProjectCalcMergeRule.INSTANCE,
      FilterToCalcRule.INSTANCE,
      ProjectToCalcRule.INSTANCE,
      CalcMergeRule.INSTANCE
  );


  static RuleSet LOGICAL_REWRITE_RULES = RuleSets.ofList(
      CalcMergeRule.INSTANCE
  );

}
