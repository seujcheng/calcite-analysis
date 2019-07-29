package com.sdu.calcite.plan.nodes;

import com.sdu.calcite.feature.FeatureContext;
import com.sdu.calcite.feature.FeatureData;
import com.sdu.calcite.plan.FeatureRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

/**
 * @author hanhan.zhang
 * */
public class FeatureTableScan extends TableScan implements FeatureRel {

    public FeatureTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public FeatureData convertTo(FeatureContext ctx) {
        return null;
    }
}
