package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.nodes.FeatureLogicalCalc;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

import static com.sdu.calcite.plan.rules.FeatureConventions.LOGICAL;

/**
 * @author hanhan.zhang
 * */
public class LogicalCalcConverter extends ConverterRule {

	public static final LogicalCalcConverter INSTANCE = new LogicalCalcConverter();

	private LogicalCalcConverter() {
		super(LogicalCalc.class, Convention.NONE, LOGICAL, "FeatureLogicalCalcConverter");
	}

	@Override
	public RelNode convert(RelNode rel) {
		LogicalCalc calc = (LogicalCalc) rel;
		RelTraitSet traitSet = rel.getTraitSet().replace(LOGICAL);

		RelNode input = calc.getInput();
		RelNode newInput = input.copy(input.getTraitSet(), input.getInputs());

		return new FeatureLogicalCalc(rel.getCluster(), traitSet, newInput, calc.getProgram());
	}


}
