package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.FeatureRel;
import com.sdu.calcite.plan.nodes.FeatureTableScan;
import com.sdu.calcite.table.FeatureTable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 *
 * @author hanhan.zhang
 * */
public class FeatureTableScanRule extends ConverterRule {

    public static final FeatureTableScanRule INSTANCE = new FeatureTableScanRule();

    private FeatureTableScanRule() {
        // RelTrait of LogicalTableScan is NONE
        super(LogicalTableScan.class, Convention.NONE, FeatureRel.CONVENTION, "FeatureTableScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        // 判断扫描表是否是 FeatureTable 类型
        FeatureTable table = scan.getTable().unwrap(FeatureTable.class);
        return table != null;
    }

    @Override
    public RelNode convert(RelNode rel) {
        // 规则匹配成功, 将其转为其他类型RelNode
        TableScan scan = (TableScan) rel;

        // TraitSet: 表示转的目标RelNode特征
        RelTraitSet traitSet = scan.getTraitSet().plus(FeatureRel.CONVENTION);

        return FeatureTableScan.create(scan.getCluster(), traitSet, scan.getTable());
    }

}
