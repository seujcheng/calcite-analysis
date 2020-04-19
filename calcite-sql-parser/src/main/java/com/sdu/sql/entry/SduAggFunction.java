package com.sdu.sql.entry;

import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.AggregateFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import lombok.Data;

@Data
public class SduAggFunction extends SduFunction {

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

  static SduAggFunction fromUserDefinedFunction(AggregateFunction<?, ?> function) {
    SduAggFunction sduAggFunction = new SduAggFunction();
    sduAggFunction.setAggregateFunction(function);
    sduAggFunction.setDeterministic(function.isDeterministic());
    sduAggFunction.setRequiresOver(function.requiresOver());
    return sduAggFunction;
  }

}
