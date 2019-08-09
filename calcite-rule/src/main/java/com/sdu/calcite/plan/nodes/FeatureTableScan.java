package com.sdu.calcite.plan.nodes;

import com.google.common.base.Preconditions;
import com.sdu.calcite.feature.fetcher.FeatureGetter;
import com.sdu.calcite.feature.fetcher.FeatureSourceGetter;
import com.sdu.calcite.plan.FeatureRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class FeatureTableScan extends TableScan implements FeatureRel {

    private FeatureTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public FeatureGetter translateToFeatureGetter() {
        // SQL只用于单表
        List<String> domainNames = table.getQualifiedName();
        Preconditions.checkState(domainNames.size() == 1);
        return FeatureSourceGetter.of(domainNames.get(0));
    }

    public static FeatureTableScan create(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        return new FeatureTableScan(cluster, traitSet, table);
    }

}
