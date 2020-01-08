package com.sdu.calcite.plan.codegen;

import org.apache.calcite.rex.*;

/**
 * @author hanhan.zhang
 * */
public abstract class CodeGenerator implements RexVisitor<GeneratedExpression> {


	@Override
	public GeneratedExpression visitInputRef(RexInputRef inputRef) {
		return null;
	}

	@Override
	public GeneratedExpression visitLocalRef(RexLocalRef localRef) {
		return null;
	}

	@Override
	public GeneratedExpression visitLiteral(RexLiteral literal) {
		return null;
	}

	@Override
	public GeneratedExpression visitCall(RexCall call) {
		return null;
	}

	@Override
	public GeneratedExpression visitOver(RexOver over) {
		return null;
	}

	@Override
	public GeneratedExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
		return null;
	}

	@Override
	public GeneratedExpression visitDynamicParam(RexDynamicParam dynamicParam) {
		return null;
	}

	@Override
	public GeneratedExpression visitRangeRef(RexRangeRef rangeRef) {
		return null;
	}

	@Override
	public GeneratedExpression visitFieldAccess(RexFieldAccess fieldAccess) {
		return null;
	}

	@Override
	public GeneratedExpression visitSubQuery(RexSubQuery subQuery) {
		return null;
	}

	@Override
	public GeneratedExpression visitTableInputRef(RexTableInputRef fieldRef) {
		return null;
	}

	@Override
	public GeneratedExpression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
		return null;
	}
}
