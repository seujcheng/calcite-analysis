package com.sdu.calcite.plan.rules;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 * @author hanhan.zhang
 * */
public class EnumerableTableToFeatureTableScan extends RelOptRule {

  public static final EnumerableTableToFeatureTableScan INSTANCE = new EnumerableTableToFeatureTableScan();

  private EnumerableTableToFeatureTableScan() {
    super(operand(EnumerableTableScan.class, any()), "EnumerableTableToFeatureTableScan");
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    EnumerableTableScan scan = call.rel(0);
    RelOptTable table = scan.getTable();
    //
    LogicalTableScan newRel = LogicalTableScan.create(scan.getCluster(), table);
    call.transformTo(newRel);
  }
}
