package com.sdu.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 * @author hanhan.zhang
 * */
public class EnumerableTableScanConvertRule extends RelOptRule {

  public static final EnumerableTableScanConvertRule INSTANCE = new EnumerableTableScanConvertRule();

  private EnumerableTableScanConvertRule() {
    super(operand(org.apache.calcite.adapter.enumerable.EnumerableTableScan.class, any()), "EnumerableTableScanConvertRule");
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    org.apache.calcite.adapter.enumerable.EnumerableTableScan scan = call.rel(0);
    RelOptTable table = scan.getTable();
    //
    LogicalTableScan newRel = LogicalTableScan.create(scan.getCluster(), table);
    call.transformTo(newRel);
  }
}
