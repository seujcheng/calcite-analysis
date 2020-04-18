package com.sdu.calcite.entry;

import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.TableFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import lombok.Data;

@Data
public class SduTableFunction extends SduFunction {

  private TableFunction<?> tableFunction;

  @Override
  public UserDefinedFunction getUserDefinedFunction() {
    return tableFunction;
  }

  @Override
  public FunctionKind getKind() {
    return null;
  }

  static SduTableFunction fromUserDefinedFunction(TableFunction<?> tableFunction) {
    SduTableFunction sduTableFunction = new SduTableFunction();
    sduTableFunction.setTableFunction(tableFunction);
    sduTableFunction.setDeterministic(tableFunction.isDeterministic());
    return sduTableFunction;
  }

}
