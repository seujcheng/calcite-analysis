package com.sdu.calcite.plan;

import com.sdu.calcite.plan.nodes.logical.SduLogicalCalcConverter;
import com.sdu.calcite.plan.rules.EnumerableToLogicalTableScan;
import com.sdu.calcite.plan.rules.SduLogicalTableScanConverter;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
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
      EnumerableToLogicalTableScan.INSTANCE);

  static RuleSet SDU_NORM_RULES = RuleSets.ofList(
      ReduceExpressionsRule.FILTER_INSTANCE,
      ReduceExpressionsRule.PROJECT_INSTANCE,
      ReduceExpressionsRule.CALC_INSTANCE,
      // ROW_NUMBER OVER构建LogicalWindow
      ProjectToWindowRule.PROJECT
  );

  static RuleSet LOGICAL_OPT_RULES = RuleSets.ofList(
      // calc rules
      FilterCalcMergeRule.INSTANCE,
      ProjectCalcMergeRule.INSTANCE,
      FilterToCalcRule.INSTANCE,
      ProjectToCalcRule.INSTANCE,
      CalcMergeRule.INSTANCE,

      SduLogicalTableScanConverter.INSTANCE,
      SduLogicalCalcConverter.INSTANCE

  );

}
