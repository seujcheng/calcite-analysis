package com.sdu.calcite;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class SduCalciteRelBuilder extends RelBuilder {

  SduCalciteRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  public RelOptPlanner getPlaner() {
    return cluster.getPlanner();
  }

}
