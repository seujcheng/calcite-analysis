package com.sdu.calcite.plan.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

import com.sdu.calcite.feature.FeatureContext;
import com.sdu.calcite.feature.FeatureData;
import com.sdu.calcite.plan.FeatureRel;
import com.sdu.calcite.table.FeatureTable;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class FeatureTableScan extends TableScan implements FeatureRel {

    private List<String> featureNames = new LinkedList<>();

    private FeatureTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    public boolean featureNamesEmpty() {
        return featureNames.isEmpty();
    }

    public void setFeatureNames(List<String> featureNames) {
        this.featureNames.addAll(featureNames);
    }

    @Override
    public CompletableFuture<FeatureData> convertTo(FeatureContext ctx) {
        FeatureTable table = getTable().unwrap(FeatureTable.class);

        // TODO: 2019-07-31

        return null;
    }

    public static FeatureTableScan create(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        return new FeatureTableScan(cluster, traitSet, table);
    }

    public static FeatureTableScan create(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, List<String> featureNames) {
        FeatureTableScan scan = create(cluster, traitSet, table);
        scan.setFeatureNames(featureNames);
        return scan;
    }

}
