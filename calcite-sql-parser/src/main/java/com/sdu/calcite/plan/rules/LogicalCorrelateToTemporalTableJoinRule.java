package com.sdu.calcite.plan.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;

public class LogicalCorrelateToTemporalTableJoinRule extends RelOptRule {

  public static final LogicalCorrelateToTemporalTableJoinRule INSTANCE = new LogicalCorrelateToTemporalTableJoinRule();

  private LogicalCorrelateToTemporalTableJoinRule() {
    super(operand(LogicalCorrelate.class, some(
        operand(RelNode.class, any()),
        operand(TableFunctionScan.class, none()))));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO: 2020-04-18
  }
}
