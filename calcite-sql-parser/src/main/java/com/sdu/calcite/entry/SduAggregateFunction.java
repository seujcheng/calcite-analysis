package com.sdu.calcite.entry;

import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.UserDefinedAggregateFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import lombok.Data;

@Data
public class SduAggregateFunction extends SduFunction {

  private UserDefinedAggregateFunction<?, ?> aggregateFunction;
  private boolean requiresOver;

  @Override
  public UserDefinedFunction getUserDefinedFunction() {
    return aggregateFunction;
  }

  @Override
  public FunctionKind getKind() {
    return FunctionKind.AGGREGATE;
  }

  static SduAggregateFunction fromUserDefinedFunction(UserDefinedAggregateFunction<?, ?> function) {
    SduAggregateFunction sduAggregateFunction = new SduAggregateFunction();
    sduAggregateFunction.setAggregateFunction(function);
    sduAggregateFunction.setDeterministic(function.isDeterministic());
    sduAggregateFunction.setRequiresOver(function.requiresOver());
    return sduAggregateFunction;
  }

}
