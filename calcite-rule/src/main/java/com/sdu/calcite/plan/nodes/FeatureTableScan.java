package com.sdu.calcite.plan.nodes;

import com.sdu.calcite.feature.FeatureStream;
import com.sdu.calcite.plan.FeatureStreamRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

/**
 * @author hanhan.zhang
 * */
public class FeatureTableScan extends TableScan implements FeatureStreamRel {

    public FeatureTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public FeatureStream convertTo() {
        return null;
    }

}
