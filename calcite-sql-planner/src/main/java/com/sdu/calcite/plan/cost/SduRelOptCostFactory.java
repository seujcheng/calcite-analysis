package com.sdu.calcite.plan.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public class SduRelOptCostFactory implements RelOptCostFactory {

  @Override
  public RelOptCost makeCost(double rowCount, double cpu, double io) {
    return new SduRelCost(rowCount, cpu, io);
  }

  @Override
  public RelOptCost makeHugeCost() {
    return SduRelCost.HUGE;
  }

  @Override
  public RelOptCost makeInfiniteCost() {
    return SduRelCost.INFINITY;
  }

  @Override
  public RelOptCost makeTinyCost() {
    return SduRelCost.TINY;
  }

  @Override
  public RelOptCost makeZeroCost() {
    return SduRelCost.ZERO;
  }

}
