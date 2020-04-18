package com.sdu.calcite.catelog;

import static com.sdu.calcite.util.UserDefinedFunctionUtils.createEvalOperandTypeChecker;
import static com.sdu.calcite.util.UserDefinedFunctionUtils.createEvalOperandTypeInference;

import com.sdu.calcite.entry.SduTableFunction;
import com.sdu.calcite.types.SduTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class SduCalciteTableFunction extends SqlUserDefinedTableFunction {

  private final SduTableFunction sduTableFunction;

  public SduCalciteTableFunction(
      String name,
      SduTypeFactory typeFactory,
      SduTableFunction sduTableFunction,
      SduTableFunctionImpl functionImpl) {
    super(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        ReturnTypes.CURSOR,
        createEvalOperandTypeInference(typeFactory, sduTableFunction),
        createEvalOperandTypeChecker(typeFactory, sduTableFunction),
        null,
        functionImpl);

    this.sduTableFunction = sduTableFunction;
  }

  @Override
  public boolean isDeterministic() {
    return sduTableFunction.isDeterministic();
  }

}
