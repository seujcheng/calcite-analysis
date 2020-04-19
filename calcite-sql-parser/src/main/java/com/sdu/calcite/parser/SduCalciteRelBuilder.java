package com.sdu.calcite.parser;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class SduCalciteRelBuilder extends RelBuilder {

  public SduCalciteRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  RelOptPlanner getPlaner() {
    return cluster.getPlanner();
  }

}
