package com.sdu.calcite.util;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

public class SduCalciteVolcanoPlanner {

  private SduCalciteVolcanoPlanner() {

  }

  public static RelNode runVolcanoPlanner(RuleSet ruleSet,
      RelNode input, RelTraitSet targetTraits, RelOptPlanner planner) {
    Program optProgram = Programs.ofRules(ruleSet);
    return optProgram.run(planner, input, targetTraits, ImmutableList.of(), ImmutableList.of());
  }

}
