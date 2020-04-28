package com.sdu.calcite.plan;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

public class SduCalciteRelBuilderFactory implements RelBuilderFactory {

  private final Context context;

  SduCalciteRelBuilderFactory(Context context) {
    this.context = context;
  }

  @Override
  public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
    return new SduCalciteRelBuilder(context, cluster, schema);
  }

}
