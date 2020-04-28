package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.nodes.logical.SduLogicalCalc;
import com.sdu.calcite.plan.nodes.logical.SduLogicalTableScan;
import com.sdu.calcite.plan.util.InputRefVisitor;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexProgram;

/*
 * 谓词下推: 减少数据量读取
 * */
public class PushProjectIntoTableScan extends RelOptRule {

  private PushProjectIntoTableScan() {
    super(operand(SduLogicalCalc.class, operand(SduLogicalTableScan.class, none())),
        "PushProjectIntoTableScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SduLogicalCalc calc = call.rel(0);
    SduLogicalTableScan scan = call.rel(1);

    RexProgram program = calc.getProgram();

    InputRefVisitor visitor = new InputRefVisitor();
    program.getProjectList()
        .stream()
        .map(program::expandLocalRef)
        .forEach(rexNode -> rexNode.accept(visitor));

    Optional<List<Integer>> selectedFields = Optional.ofNullable(visitor.getFieldIndexes());
    SduLogicalTableScan newScan = scan.copy(scan.getTraitSet(), scan.getInputs(), selectedFields);


  }

}
