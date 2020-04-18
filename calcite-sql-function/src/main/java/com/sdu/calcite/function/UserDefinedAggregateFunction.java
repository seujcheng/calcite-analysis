package com.sdu.calcite.function;

public abstract class UserDefinedAggregateFunction<T, ACC> extends UserDefinedFunction {

  public abstract ACC createAccumulator();

  @Override
  public FunctionKind getKind() {
    return FunctionKind.AGGREGATE;
  }
}
