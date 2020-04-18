package com.sdu.calcite.entry;

import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.TableFunction;

public class SduTableFunction extends SduFunction {

  @Override
  public FunctionKind getKind() {
    return null;
  }

  static SduTableFunction fromUserDefinedFunction(TableFunction<?> tableFunction) {
    throw new RuntimeException();
  }

}
