package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.nodes.SduConventions;
import com.sdu.calcite.plan.nodes.logical.SduLogicalTableScan;
import java.util.Optional;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class SduLogicalTableScanConverter extends ConverterRule {

  public static final SduLogicalTableScanConverter INSTANCE = new SduLogicalTableScanConverter();

  private SduLogicalTableScanConverter() {
    super(LogicalTableScan.class,
        Convention.NONE,
        SduConventions.LOGICAL,
        "SduLogicalTableScanConverter");
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalTableScan scan = (LogicalTableScan) rel;
    RelTraitSet traitSet = scan.getTraitSet().replace(SduConventions.LOGICAL);

    return new SduLogicalTableScan(rel.getCluster(),
        traitSet,
        scan.getTable(),
        Optional.empty());
  }

}
