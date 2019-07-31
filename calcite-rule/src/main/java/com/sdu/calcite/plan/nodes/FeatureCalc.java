package com.sdu.calcite.plan.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexProgram;

public class FeatureCalc extends Calc {

  public FeatureCalc(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram program) {
    super(cluster, traits, child, program);
  }

  @Override
  public Calc copy(final RelTraitSet traitSet, final RelNode child, final RexProgram program) {
    return null;
  }


}
