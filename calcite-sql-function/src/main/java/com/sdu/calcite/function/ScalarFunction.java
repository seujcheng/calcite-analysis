package com.sdu.calcite.function;

public abstract class ScalarFunction extends UserDefinedFunction {

  @Override
  public void open(FunctionContext context) throws Exception {

  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public FunctionKind getKind() {
    return FunctionKind.SCALAR;
  }

}
