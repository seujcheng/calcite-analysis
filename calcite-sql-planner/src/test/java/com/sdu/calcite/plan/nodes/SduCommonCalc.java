package com.sdu.calcite.plan.nodes;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

public interface SduCommonCalc {

  default RelOptCost computeSelfCost(RexProgram calcProgram, RelOptPlanner planner, double rowCnt) {
    long computationCnt = calcProgram.getExprList()
        .stream()
        .filter(SduCommonCalc::isComputation)
        .count();

    double newRowCnt = estimateRowCount(calcProgram, rowCnt);

    return planner.getCostFactory().makeCost(newRowCnt,
        newRowCnt * computationCnt,
        0);
  }

  default double estimateRowCount(RexProgram calcProgram, double rowCnt) {
    if (calcProgram.getCondition() != null) {
      // 过滤条件, 数据量减少
      RexNode condition = calcProgram.expandLocalRef(calcProgram.getCondition());
      double selectivity = RelMdUtil.guessSelectivity(condition, false);
      return Math.max(1.0, rowCnt * selectivity);
    }
    return rowCnt;
  }

  static boolean isComputation(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {   // $1
      return false;
    }
    if (rexNode instanceof RexLiteral) {    // 常量
      return false;
    }
    if (rexNode instanceof RexCall) {
      RexCall call = (RexCall) rexNode;
      return !call.getOperator().getName().equals("CAST");
    }
    return true;
  }

}
