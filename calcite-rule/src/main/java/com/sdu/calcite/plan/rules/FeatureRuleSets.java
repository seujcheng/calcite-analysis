package com.sdu.calcite.plan.rules;

import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * @author hanhan.zhang
 * */
public class FeatureRuleSets {

  // 子查询优化
  public static final RuleSet TABLE_SUBQUERY_RULES = RuleSets.ofList(
      SubQueryRemoveRule.FILTER,
      SubQueryRemoveRule.PROJECT,
      SubQueryRemoveRule.JOIN
  );

  //
  public static final RuleSet LOGICAL_OPT_RULES = RuleSets.ofList(

      // Calc rules
      FilterCalcMergeRule.INSTANCE,
      ProjectCalcMergeRule.INSTANCE,
      FilterToCalcRule.INSTANCE,
      ProjectToCalcRule.INSTANCE,
      CalcMergeRule.INSTANCE,

      LogicalCalcConverter.INSTANCE

  );

  // TableScan
  public static final RuleSet TABLE_SCAN_RULES = RuleSets.ofList(
      EnumerableTableToFeatureTableScan.INSTANCE,
      FeatureTableScanRule.INSTANCE
  );

  private FeatureRuleSets() {}

}
