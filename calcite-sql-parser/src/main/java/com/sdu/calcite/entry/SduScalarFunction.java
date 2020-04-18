package com.sdu.calcite.entry;

import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.ScalarFunction;
import lombok.Data;


@Data
public class SduScalarFunction extends SduFunction{

  private ScalarFunction scalarFunction;

  @Override
  public FunctionKind getKind() {
    return FunctionKind.SCALAR;
  }

  static SduScalarFunction fromUserDefinedFunction(ScalarFunction scalarFunction) {
    SduScalarFunction sduScalarFunction = new SduScalarFunction();
    sduScalarFunction.setScalarFunction(scalarFunction);
    sduScalarFunction.setDeterministic(scalarFunction.isDeterministic());
    return sduScalarFunction;
  }
}
