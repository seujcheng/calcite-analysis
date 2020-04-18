package com.sdu.calcite.util;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;

public class SduCalciteHepPlanner {

  private SduCalciteHepPlanner() {

  }

  /**
   * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
   * node before going to the next one. If a rule creates a new node all of the rules will
   * be applied to this new node.
   * */
  public static RelNode runHepPlannerSimultaneously(
      HepMatchOrder hepMatchOrder ,
      RuleSet ruleSet,
      RelNode input,
      RelTraitSet targetTraits) {

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addMatchOrder(hepMatchOrder);
    builder.addRuleCollection(Lists.newArrayList(ruleSet.iterator()));
    return runHepPlanner(builder.build(), input, targetTraits);
  }

  /**
   * run HEP planner with rules applied one by one. First apply one rule to all of the nodes
   * and only then apply the next rule. If a rule creates a new node preceding rules will not
   * be applied to the newly created node.
   * */
  public static RelNode runHepPlannerSequentially(
      HepMatchOrder hepMatchOrder,
      RuleSet ruleSet,
      RelNode input,
      RelTraitSet targetTraits) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addMatchOrder(hepMatchOrder);
    for (RelOptRule relOptRule : ruleSet) {
      builder.addRuleInstance(relOptRule);
    }
    return runHepPlanner(builder.build(), input, targetTraits);
  }



  private static RelNode runHepPlanner(HepProgram hepProgram, RelNode input, RelTraitSet targetTraits) {
    HepPlanner planner = new HepPlanner(hepProgram);
    planner.setRoot(input);
    if (input.getTraitSet() != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify());
    }
    return planner.findBestExp();
  }


}
