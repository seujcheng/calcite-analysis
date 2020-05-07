package com.sdu.calcite.plan;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;

public interface SduRelOptimizerFactory {


  SduRelOptimizer createOptimizer(Context context, RelOptPlanner planner);

}
