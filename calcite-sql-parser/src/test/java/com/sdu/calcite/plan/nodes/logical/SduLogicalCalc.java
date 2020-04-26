package com.sdu.calcite.plan.nodes.logical;

import com.sdu.calcite.plan.nodes.SduCommonCalc;
import com.sdu.calcite.plan.nodes.SduLogicalRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

public class SduLogicalCalc extends Calc implements SduLogicalRel, SduCommonCalc {

  public SduLogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexProgram program) {
    super(cluster, traits, child, program);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
    return new SduLogicalCalc(getCluster(),
        traitSet,
        child,
        program);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double rowCnt = mq.getRowCount(this.getInput());
    return computeSelfCost(program, planner, rowCnt);
  }
}
