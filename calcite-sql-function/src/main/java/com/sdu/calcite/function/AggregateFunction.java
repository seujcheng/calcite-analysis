package com.sdu.calcite.function;

public abstract class AggregateFunction<T, ACC> extends UserDefinedFunction {

  public abstract ACC createAccumulator();

  public abstract boolean requiresOver();

  public abstract Class<T> getResultType();

  public abstract Class<ACC> getAccumulatorType();

  @Override
  public FunctionKind getKind() {
    return FunctionKind.AGGREGATE;
  }

}
