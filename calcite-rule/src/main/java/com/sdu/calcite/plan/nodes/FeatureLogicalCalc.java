package com.sdu.calcite.plan.nodes;

import com.sdu.calcite.feature.fetcher.FeatureCalGetter;
import com.sdu.calcite.feature.fetcher.FeatureGetter;
import com.sdu.calcite.plan.FeatureRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexProgram;

public class FeatureLogicalCalc extends Calc implements FeatureRel {

	public FeatureLogicalCalc(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram program) {
		super(cluster, traits, child, program);
	}

	@Override
	public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
		return new FeatureLogicalCalc(getCluster(), traitSet, child, program);
	}

	@Override
	public FeatureGetter translateToFeatureGetter() {
		FeatureRel input = (FeatureRel) getInput();
		FeatureGetter featureGetter = input.translateToFeatureGetter();

		// TODO: 处理逻辑

		return FeatureCalGetter.of(featureGetter);
	}
}
