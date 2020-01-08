package com.sdu.calcite.plan;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

/**
 * @author hanhan.zhang
 * */
public class FeatureRelBuilder extends RelBuilder {

    public FeatureRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public RelOptPlanner getPlaner() {
        return cluster.getPlanner();
    }

}
