package com.sdu.calcite.entry;

import com.sdu.calcite.function.FunctionKind;
import org.apache.calcite.schema.AggregateFunction;

public class SduAggregateFunction extends SduFunction {

  @Override
  public FunctionKind getKind() {
    return FunctionKind.AGGREGATE;
  }

  static SduAggregateFunction fromUserDefinedFunction(AggregateFunction function) {
    throw new RuntimeException();
  }

}
