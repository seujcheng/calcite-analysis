package com.sdu.calcite.entry;

import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.AggregateFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import lombok.Data;

@Data
public class SduAggregateFunction extends SduFunction {

  private AggregateFunction<?, ?> aggregateFunction;
  private boolean requiresOver;

  @Override
  public UserDefinedFunction getUserDefinedFunction() {
    return aggregateFunction;
  }

  @Override
  public FunctionKind getKind() {
    return FunctionKind.AGGREGATE;
  }

  static SduAggregateFunction fromUserDefinedFunction(AggregateFunction<?, ?> function) {
    SduAggregateFunction sduAggregateFunction = new SduAggregateFunction();
    sduAggregateFunction.setAggregateFunction(function);
    sduAggregateFunction.setDeterministic(function.isDeterministic());
    sduAggregateFunction.setRequiresOver(function.requiresOver());
    return sduAggregateFunction;
  }

}
