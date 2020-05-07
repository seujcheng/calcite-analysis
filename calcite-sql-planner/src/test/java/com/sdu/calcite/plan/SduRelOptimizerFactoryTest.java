package com.sdu.calcite.plan;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;

public class SduRelOptimizerFactoryTest implements SduRelOptimizerFactory {

  @Override
  public SduRelOptimizer createOptimizer(Context context, RelOptPlanner planner) {
    return new SduRelOptimizerTest(context, planner);
  }

}

