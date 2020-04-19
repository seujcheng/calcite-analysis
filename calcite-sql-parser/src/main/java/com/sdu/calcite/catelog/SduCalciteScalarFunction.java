package com.sdu.calcite.catelog;

import static com.sdu.sql.parse.UserDefinedFunctionUtils.createEvalOperandTypeChecker;
import static com.sdu.sql.parse.UserDefinedFunctionUtils.createEvalOperandTypeInference;
import static com.sdu.sql.parse.UserDefinedFunctionUtils.getEvalMethod;

import com.sdu.sql.entry.SduScalarFunction;
import com.sdu.calcite.SduCalciteTypeFactory;
import java.lang.reflect.Method;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class SduCalciteScalarFunction extends SqlFunction {

  private final SduScalarFunction function;

  public SduCalciteScalarFunction(SduCalciteTypeFactory typeFactory, String name, SduScalarFunction function) {
    super(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        createReturnTypeInference(typeFactory, function),
        createEvalOperandTypeInference(typeFactory, function),
        createEvalOperandTypeChecker(typeFactory, function),
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
    this.function = function;
  }


  @Override
  public boolean isDeterministic() {
    return function.isDeterministic();
  }

  private static SqlReturnTypeInference createReturnTypeInference(SduCalciteTypeFactory typeFactory, SduScalarFunction function) {

    class ScalarFunctionReturnTypeInference implements SqlReturnTypeInference {

      private final SduCalciteTypeFactory typeFactory;
      private final SduScalarFunction function;

      private ScalarFunctionReturnTypeInference(SduCalciteTypeFactory typeFactory, SduScalarFunction function) {
        this.typeFactory = typeFactory;
        this.function = function;
      }

      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        /*
         * 1: 获取SQL调用UDF输入参数
         *
         * 2: 根据输出参数在UDF中查找是否存在eval函数
         * */
        Class<?>[] parameters = opBinding.collectOperandTypes().stream()
            .map(typeFactory::getJavaClass)
            .toArray(Class<?>[]::new);

        Method method = getEvalMethod(function.getScalarFunction(), parameters);

        return typeFactory.createSqlType(method.getReturnType().getName());
      }
    }

    return new ScalarFunctionReturnTypeInference(typeFactory, function);
  }

}
