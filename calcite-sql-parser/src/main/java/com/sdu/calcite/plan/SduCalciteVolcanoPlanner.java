package com.sdu.calcite.plan;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

class SduCalciteVolcanoPlanner {

  private SduCalciteVolcanoPlanner() {

  }

  static RelNode runVolcanoPlanner(RuleSet ruleSet,
      RelNode input, RelTraitSet targetTraits, RelOptPlanner planner) {
    /*
     * https://zhuanlan.zhihu.com/p/48735419
     *
     * VolcanoPlanner是基于成本的优化算法, 通过剪枝和缓冲中间结果(动态规划)的方法降低计算消耗
     *
     *
     * */
    Program optProgram = Programs.ofRules(ruleSet);
    return optProgram.run(planner, input, targetTraits, ImmutableList.of(), ImmutableList.of());
  }

}
