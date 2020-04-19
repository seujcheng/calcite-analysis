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
import java.util.List;
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
    RelNode convSubQueryPlan = optimizeConvertSubQueries(relNode);
    RelNode expandedPlan = optimizeExpandPlan(convSubQueryPlan);
    RelNode decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan, builder);
    RelNode normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan, calciteConfig);
    RelNode logicalPlan = optimizeLogicalPlan(normalizedPlan, calciteConfig, builder);
    RelNode logicalRewritePlan = optimizeLogicalRewritePlan(logicalPlan, calciteConfig);
    return optimizePhysicalPlan(logicalRewritePlan, calciteConfig, builder);
  }

  private static RelNode optimizeConvertSubQueries(RelNode input) {
    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        SduCalciteRuleSets.TABLE_SUBQUERY_RULES, input, input.getTraitSet());
  }

  private static RelNode optimizeExpandPlan(RelNode input) {
    RelNode result = runHepPlannerSimultaneously(HepMatchOrder.TOP_DOWN,
        SduCalciteRuleSets.EXPAND_PLAN_RULES, input, input.getTraitSet());

    return runHepPlannerSequentially(HepMatchOrder.DEPTH_FIRST,
        SduCalciteRuleSets.POST_EXPAND_CLEAN_UP_RULES, result, result.getTraitSet());
  }

  private static RelNode optimizeNormalizeLogicalPlan(RelNode input, SduCalciteConfig config) {
    RuleSet normRuleSet = ofRuleSet(config.getNormRuleSet(), SDU_NORM_RULES, config.replacesNormRuleSet());

    return runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP,
        normRuleSet, input, input.getTraitSet());
  }

  private static RelNode optimizeLogicalPlan(RelNode input, SduCalciteConfig config, SduCalciteRelBuilder builder) {
    RuleSet logicalRuleSet = ofRuleSet(config.getLogicalOptRuleSet(), LOGICAL_OPT_RULES, config.replacesLogicalOptRuleSet());

    // TODO: 2020-04-19 这里缺少Convention, VolcanoPlaner无法优化, 待排查

//    return runVolcanoPlanner(logicalRuleSet, input, input.getTraitSet(), builder.getPlaner());

    return runHepPlannerSequentially(HepMatchOrder.TOP_DOWN, logicalRuleSet,
        input, input.getTraitSet());
  }

  private static RelNode optimizeLogicalRewritePlan(RelNode relNode, SduCalciteConfig config) {
    RuleSet rewriteRuleSet = ofRuleSet(config.getLogicalRewriteRuleSet(), LOGICAL_REWRITE_RULES, config.replacesLogicalRewriteRuleSet());

    return runHepPlannerSequentially(
        HepMatchOrder.TOP_DOWN,
        rewriteRuleSet, relNode, relNode.getTraitSet());
  }

  private static RelNode optimizePhysicalPlan(RelNode input, SduCalciteConfig config, SduCalciteRelBuilder builder) {
    RuleSet physicalOptRuleSet = config.getPhysicalRuleSet();
    if (physicalOptRuleSet == null || !physicalOptRuleSet.iterator().hasNext()) {
      return input;
    }

    RelTrait physicalRelTrait = config.getPhysicalRelTrait();
    RelTraitSet physicalOutputProps = input.getTraitSet();
    if (physicalRelTrait != null) {
      physicalOutputProps = input.getTraitSet().replace(physicalRelTrait).simplify();
    }

    return runVolcanoPlanner(physicalOptRuleSet, input, physicalOutputProps, builder.getPlaner());
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
