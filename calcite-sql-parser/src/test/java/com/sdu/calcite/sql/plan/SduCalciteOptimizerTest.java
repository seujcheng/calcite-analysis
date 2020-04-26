package com.sdu.calcite.sql.plan;

import com.sdu.calcite.plan.SduCalciteOptimizer;
import com.sdu.calcite.plan.SduCalcitePlanningConfigBuilder;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;

public class SduCalciteOptimizerTest extends SduCalciteOptimizer {

  public SduCalciteOptimizerTest(SduCalcitePlanningConfigBuilder calcitePlanningConfigBuilder) {
    super(calcitePlanningConfigBuilder);
  }

  @Override
  public RelNode optimize(RelNode relNode, RelBuilder relBuilder) {
    RelNode convSubQueryPlan = optimizeConvertSubQueries(relNode);
    RelNode expandedPlan = optimizeExpandPlan(convSubQueryPlan);
    RelNode decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan, relBuilder);
    RelNode normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan);
    RelNode logicalPlan = optimizeLogicalPlan(normalizedPlan);
    return optimizeLogicalRewritePlan(logicalPlan);
  }

  private RelNode optimizeConvertSubQueries(RelNode input) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.TABLE_SUBQUERY_RULES, input, input.getTraitSet());
  }

  private RelNode optimizeExpandPlan(RelNode input) {
    RelNode result = runHepPlannerSimultaneously(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.EXPAND_PLAN_RULES, input, input.getTraitSet());

    return runHepPlannerSequentially(HepMatchOrder.DEPTH_FIRST,
        SduCalciteRuleSets.POST_EXPAND_CLEAN_UP_RULES, result, result.getTraitSet());
  }

  private RelNode optimizeNormalizeLogicalPlan(RelNode input) {

    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.SDU_NORM_RULES, input, input.getTraitSet());
  }

  private RelNode optimizeLogicalPlan(RelNode input) {
    // TODO: 2020-04-19 这里缺少Convention, VolcanoPlaner无法优化, 待排查
//    SduCalciteRelBuilder builder = context.unwrap(SduCalciteRelBuilder.class);
//    runVolcanoPlanner(logicalRuleSet, input, input.getTraitSet(), builder.getPlaner());

    return runHepPlannerSequentially(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.LOGICAL_OPT_RULES, input, input.getTraitSet());
  }

  private RelNode optimizeLogicalRewritePlan(RelNode relNode) {
    return runHepPlannerSequentially(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.LOGICAL_REWRITE_RULES, relNode, relNode.getTraitSet());
  }


}
