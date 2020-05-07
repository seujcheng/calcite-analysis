package com.sdu.calcite.plan;

import com.sdu.calcite.plan.nodes.SduConventions;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;

class SduRelOptimizerTest extends SduRelOptimizer {

  SduRelOptimizerTest(Context context, RelOptPlanner planner) {
    super(context, planner);
  }

  @Override
  public RelNode optimize(RelNode relNode) {
    RelNode convSubQueryPlan = optimizeConvertSubQueries(relNode);
    RelNode expandedPlan = optimizeExpandPlan(convSubQueryPlan);
//    RelNode decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan, relBuilder);
    RelNode normalizedPlan = optimizeNormalizeLogicalPlan(expandedPlan);
    return optimizeLogicalPlan(normalizedPlan);
  }

  private RelNode optimizeConvertSubQueries(RelNode input) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.TABLE_SUBQUERY_RULES, input, input.getTraitSet());
  }

  private RelNode optimizeExpandPlan(RelNode input) {
    return runHepPlannerSimultaneously(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.EXPAND_PLAN_RULES, input, input.getTraitSet());
  }

  private RelNode optimizeNormalizeLogicalPlan(RelNode input) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.SDU_NORM_RULES, input, input.getTraitSet());
  }

  private RelNode optimizeLogicalPlan(RelNode input) {
    /*
     * https://issues.apache.org/jira/browse/CALCITE-3255
     * 1: set up desired conventions in target traits
     *
     * 2: define transform converter
     * */
    RelTraitSet traitSet = input.getTraitSet().replace(SduConventions.LOGICAL).simplify();
    return runVolcanoPlanner(SduCalciteRuleSets.LOGICAL_OPT_RULES,
        input, traitSet);
  }


}
