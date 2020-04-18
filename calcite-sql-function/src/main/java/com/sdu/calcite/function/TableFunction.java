package com.sdu.calcite.function;

public abstract class TableFunction<T> extends UserDefinedFunction {

  public abstract void collect(T row);

  public abstract String[] getColumnNames();

  public abstract Class<?>[] getColumnTypes();

  @Override
  public FunctionKind getKind() {
    return FunctionKind.TABLE;
  }

}
