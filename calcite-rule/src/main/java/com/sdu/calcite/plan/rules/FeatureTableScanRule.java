package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.nodes.Conventions;
import com.sdu.calcite.plan.nodes.FeatureTableScan;
import com.sdu.calcite.table.FeatureStreamTable;
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

    public FeatureTableScanRule() {
        super(LogicalTableScan.class, Convention.NONE, Conventions.FEATURESTREAM, "FeatureTableScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        // 判断扫描表是否是 FeatureStreamTable 类型
        FeatureStreamTable table = scan.getTable().unwrap(FeatureStreamTable.class);
        return table != null;
    }

    @Override
    public RelNode convert(RelNode rel) {
        // 规则匹配成功, 将其转为其他类型RelNode
        TableScan scan = (TableScan) rel;

        // TraitSet: 表示转的目标RelNode特征
        RelTraitSet traitSet = scan.getTraitSet().replace(Conventions.FEATURESTREAM);

        return new FeatureTableScan(scan.getCluster(), traitSet, scan.getTable());
    }

}
