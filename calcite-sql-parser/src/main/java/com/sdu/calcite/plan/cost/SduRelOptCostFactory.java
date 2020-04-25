package com.sdu.calcite.plan.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public class SduRelOptCostFactory implements RelOptCostFactory {

  @Override
  public RelOptCost makeCost(double rowCount, double cpu, double io) {
    return new SduCalciteCost(rowCount, cpu, io);
  }

  @Override
  public RelOptCost makeHugeCost() {
    return SduCalciteCost.HUGE;
  }

  @Override
  public RelOptCost makeInfiniteCost() {
    return SduCalciteCost.INFINITY;
  }

  @Override
  public RelOptCost makeTinyCost() {
    return SduCalciteCost.TINY;
  }

  @Override
  public RelOptCost makeZeroCost() {
    return SduCalciteCost.ZERO;
  }

}
