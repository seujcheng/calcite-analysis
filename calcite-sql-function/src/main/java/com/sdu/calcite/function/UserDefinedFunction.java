package com.sdu.calcite.function;

import java.io.Serializable;

public abstract class UserDefinedFunction implements Serializable {

  public abstract void open(FunctionContext context) throws Exception;

  public abstract void close() throws Exception;

  public abstract FunctionKind getKind();

  public boolean isDeterministic() {
    return true;
  }

}
