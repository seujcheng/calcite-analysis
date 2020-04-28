package com.sdu.calcite.plan.nodes.logical;

import com.sdu.calcite.plan.nodes.SduConventions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public class SduLogicalCalcConverter extends ConverterRule {

  public static final SduLogicalCalcConverter INSTANCE = new SduLogicalCalcConverter();

  private SduLogicalCalcConverter() {
    super(LogicalCalc.class,
        Convention.NONE,
        SduConventions.LOGICAL,
        "SduLogicalCalcConverter");
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalCalc calc = (LogicalCalc) rel;
    RelTraitSet traitSet = calc.getTraitSet().replace(SduConventions.LOGICAL);

    RelNode newInput = RelOptRule.convert(calc.getInput(), SduConventions.LOGICAL);

    return new SduLogicalCalc(calc.getCluster(),
        traitSet,
        newInput,
        calc.getProgram());
  }
}
