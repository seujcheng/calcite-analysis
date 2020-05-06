package com.sdu.calcite;

import com.sdu.calcite.plan.SduCalciteOptimizer;
import com.sdu.calcite.plan.SduCalcitePlannerContext;
import com.sdu.calcite.plan.SduCalciteRelBuilder;
import com.sdu.calcite.plan.SduCalciteSqlPlanner;
import java.util.Objects;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SduPlanner {

  private final Logger LOG = LoggerFactory.getLogger(SduPlanner.class);

  private final SduCalcitePlannerContext calcitePlannerContext;

  private final SduCalciteOptimizer calciteOptimizer;

  public SduPlanner(
      SduCalcitePlannerContext calcitePlannerContext,
      Function<SduCalcitePlannerContext, SduCalciteOptimizer> calciteOptimizerSupplier) {
    this.calcitePlannerContext = Objects.requireNonNull(calcitePlannerContext);
    this.calciteOptimizer = calciteOptimizerSupplier.apply(calcitePlannerContext);
  }

  public RelNode optimizePlan(SqlNode sqlNode) {
    SduCalciteSqlPlanner planner = calcitePlannerContext.createCalcitePlanner();
    SduCalciteRelBuilder relBuilder = calcitePlannerContext.createCalciteRelBuilder();

    RelRoot relRoot = planner.validateAndRel(sqlNode);
    RelNode relNode = calciteOptimizer.optimize(relRoot.rel, relBuilder);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}", RelOptUtil.toString(relNode));
    }
    return relNode;
  }

}
