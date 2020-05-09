package com.sdu.calcite.plan;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class SduCalciteRelBuilder extends RelBuilder {

  SduCalciteRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  public static SduCalciteRelBuilder of(RelOptCluster cluster, RelOptSchema optSchema) {
    Context context = cluster.getPlanner().getContext();
    return new SduCalciteRelBuilder(context, cluster, optSchema);
  }

}
