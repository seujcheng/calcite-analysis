package com.sdu.calcite;

import com.sdu.calcite.plan.SduCalciteOptimizer;
import com.sdu.calcite.plan.SduCalcitePlanningConfigBuilder;
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

  private final SduCalcitePlanningConfigBuilder calcitePlanningConfigBuilder;

  private final SduCalciteOptimizer calciteOptimizer;

  public SduPlanner(
      SduCalcitePlanningConfigBuilder calcitePlanningConfigBuilder,
      Function<SduCalcitePlanningConfigBuilder, SduCalciteOptimizer> calciteOptimizerSupplier) {
    this.calcitePlanningConfigBuilder = Objects.requireNonNull(calcitePlanningConfigBuilder);
    this.calciteOptimizer = calciteOptimizerSupplier.apply(calcitePlanningConfigBuilder);
  }

  public RelNode optimizePlan(SqlNode sqlNode) {
    SduCalciteSqlPlanner planner = calcitePlanningConfigBuilder.createCalcitePlanner();
    SduCalciteRelBuilder relBuilder = calcitePlanningConfigBuilder.createCalciteRelBuilder();

    RelRoot relRoot = planner.validateAndRel(sqlNode);
    RelNode relNode = calciteOptimizer.optimize(relRoot.rel, relBuilder);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}", RelOptUtil.toString(relNode));
    }
    return relNode;
  }

}
