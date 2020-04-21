package com.sdu.calcite.plan;

import static com.sdu.calcite.plan.SduCalciteHepPlanner.runHepPlannerSequentially;
import static com.sdu.calcite.plan.SduCalciteHepPlanner.runHepPlannerSimultaneously;
import static com.sdu.calcite.plan.SduCalciteRuleSets.LOGICAL_OPT_RULES;
import static com.sdu.calcite.plan.SduCalciteRuleSets.LOGICAL_REWRITE_RULES;
import static com.sdu.calcite.plan.SduCalciteRuleSets.SDU_NORM_RULES;
import static com.sdu.calcite.plan.SduCalciteVolcanoPlanner.runVolcanoPlanner;

import com.google.common.collect.Lists;
import com.sdu.calcite.SduCalciteConfig;
import com.sdu.calcite.SduCalciteRelBuilder;
import com.sdu.calcite.SduCalciteRuleSetConfig;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class SduCalciteSqlOptimizer {

  private final SduCalciteConfig calciteConfig;

  public SduCalciteSqlOptimizer(SduCalciteConfig calciteConfig) {
    this.calciteConfig = calciteConfig;
  }

  public RelNode optimize(RelNode relNode, SduCalciteRelBuilder builder) {
    Context context = Contexts.of(calciteConfig, builder);

    /*
     *
     * */
    RelNode convSubQueryPlan = optimizeConvertSubQueries(relNode, context);
    RelNode expandedPlan = optimizeExpandPlan(convSubQueryPlan, context);
    RelNode decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan, builder);
    RelNode normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan, context);

    RelNode logicalPlan = optimizeLogicalPlan(normalizedPlan, context);
    RelNode logicalRewritePlan = optimizeLogicalRewritePlan(logicalPlan, context);
    return optimizePhysicalPlan(logicalRewritePlan, context);
  }

  private static RelNode optimizeConvertSubQueries(RelNode input, Context context) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.TABLE_SUBQUERY_RULES, input, input.getTraitSet(), context);
  }

  private static RelNode optimizeExpandPlan(RelNode input, Context context) {
    RelNode result = runHepPlannerSimultaneously(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.EXPAND_PLAN_RULES, input, input.getTraitSet(), context);

    return runHepPlannerSequentially(HepMatchOrder.DEPTH_FIRST,
        SduCalciteRuleSets.POST_EXPAND_CLEAN_UP_RULES, result, result.getTraitSet(), context);
  }

  private static RelNode optimizeNormalizeLogicalPlan(RelNode input, Context context) {
    SduCalciteRuleSetConfig conf = context.unwrap(SduCalciteConfig.class).getRuleSetConfig();
    RuleSet normRuleSet = ofRuleSet(conf.getNormRuleSet(), SDU_NORM_RULES, conf.replacesNormRuleSet());

    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        normRuleSet, input, input.getTraitSet(), context);
  }

  private static RelNode optimizeLogicalPlan(RelNode input, Context context) {
    SduCalciteRuleSetConfig conf = context.unwrap(SduCalciteConfig.class).getRuleSetConfig();
    RuleSet logicalRuleSet = ofRuleSet(conf.getLogicalOptRuleSet(), LOGICAL_OPT_RULES, conf.replacesLogicalOptRuleSet());

    // TODO: 2020-04-19 这里缺少Convention, VolcanoPlaner无法优化, 待排查

//    return runVolcanoPlanner(logicalRuleSet, input, input.getTraitSet(), builder.getPlaner());

    return runHepPlannerSequentially(HepMatchOrder.TOP_DOWN, logicalRuleSet,
        input, input.getTraitSet(), context);
  }

  private static RelNode optimizeLogicalRewritePlan(RelNode relNode, Context context) {
    SduCalciteRuleSetConfig conf = context.unwrap(SduCalciteConfig.class).getRuleSetConfig();
    RuleSet rewriteRuleSet = ofRuleSet(conf.getLogicalRewriteRuleSet(), LOGICAL_REWRITE_RULES, conf.replacesLogicalRewriteRuleSet());

    return runHepPlannerSequentially(
        HepMatchOrder.TOP_DOWN,
        rewriteRuleSet, relNode, relNode.getTraitSet(), context);
  }

  private static RelNode optimizePhysicalPlan(RelNode input, Context context) {
    SduCalciteRuleSetConfig conf = context.unwrap(SduCalciteConfig.class).getRuleSetConfig();

    RuleSet physicalOptRuleSet = conf.getPhysicalRuleSet();
    if (physicalOptRuleSet == null || !physicalOptRuleSet.iterator().hasNext()) {
      return input;
    }

    RelTrait physicalRelTrait = conf.getPhysicalRelTrait();
    RelTraitSet physicalOutputProps = input.getTraitSet();
    if (physicalRelTrait != null) {
      physicalOutputProps = input.getTraitSet().replace(physicalRelTrait).simplify();
    }

    return runVolcanoPlanner(physicalOptRuleSet, input, physicalOutputProps, context.unwrap(RelOptPlanner.class));
  }

  private static RuleSet ofRuleSet(RuleSet first, RuleSet second, boolean replace) {
    if (first == null || !first.iterator().hasNext()) {
      return second;
    }

    if (replace) {
      return first;
    }

    List<RelOptRule> total = Lists.newLinkedList(first);
    for (RelOptRule relOptRule : second) {
      total.add(relOptRule);
    }

    return RuleSets.ofList(total);
  }
}
