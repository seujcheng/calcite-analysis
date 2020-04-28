package com.sdu.calcite.plan;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.metadata.SduCalciteRelMdRowCount;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;

class SduCalciteRelOptClusterFactory {

  private static final RelMetadataProvider SOURCE = ChainedRelMetadataProvider.of(
      ImmutableList.of(
          SduCalciteRelMdRowCount.SOURCE,
          DefaultRelMetadataProvider.INSTANCE
      )
  );

  private SduCalciteRelOptClusterFactory() {
  }

  static RelOptCluster create(RelOptPlanner planner, RexBuilder builder) {
    RelOptCluster cluster = RelOptCluster.create(planner, builder);
    cluster.setMetadataProvider(SOURCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(cluster.getMetadataProvider()));
    return cluster;
  }

}
