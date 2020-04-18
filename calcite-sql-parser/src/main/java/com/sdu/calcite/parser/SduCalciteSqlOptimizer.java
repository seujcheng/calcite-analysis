package com.sdu.calcite.parser;

import static com.sdu.calcite.util.SduCalciteHepPlanner.runHepPlannerSequentially;
import static com.sdu.calcite.util.SduCalciteHepPlanner.runHepPlannerSimultaneously;

import com.sdu.calcite.util.SduCalciteRuleSets;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;

public class SduCalciteSqlOptimizer {


  public RelNode optimize(RelNode relNode, SduCalciteRelBuilder builder) {
    RelNode convSubQueryPlan = optimizeConvertSubQueries(relNode);
    RelNode expandedPlan = optimizeExpandPlan(convSubQueryPlan);
    RelNode normalizedPlan = optimizeNormalizeLogicalPlan(expandedPlan);
    RelNode logicalPlan = optimizeLogicalPlan(normalizedPlan, builder);
    return optimizeLogicalRewritePlan(logicalPlan);
  }

  private static RelNode optimizeConvertSubQueries(RelNode input) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.TABLE_SUBQUERY_RULES, input, input.getTraitSet());
  }

  private static RelNode optimizeExpandPlan(RelNode input) {
    return runHepPlannerSimultaneously(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.EXPAND_PLAN_RULES, input, input.getTraitSet());
  }

  private static RelNode optimizeNormalizeLogicalPlan(RelNode input) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.SDU_NORM_RULES, input, input.getTraitSet());
  }

  private static RelNode optimizeLogicalPlan(RelNode input, SduCalciteRelBuilder builder){
//    return runVolcanoPlanner(SduCalciteRuleSets.LOGICAL_OPT_RULES,
//        input, input.getTraitSet(), builder.getPlaner());
    return runHepPlannerSequentially(HepMatchOrder.TOP_DOWN, SduCalciteRuleSets.LOGICAL_OPT_RULES,
        input, input.getTraitSet());
  }

  private static RelNode optimizeLogicalRewritePlan(RelNode relNode) {
    return runHepPlannerSequentially(
        HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.LOGICAL_REWRITE_RULES, relNode, relNode.getTraitSet());
  }
}
