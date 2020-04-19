package com.sdu.calcite.catelog;

import static com.sdu.sql.parse.UserDefinedFunctionUtils.getAccumulateMethod;

import com.sdu.sql.entry.SduAggFunction;
import com.sdu.calcite.SduCalciteTypeFactory;
import java.lang.reflect.Method;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;

public class SduCalciteAggFunction extends SqlUserDefinedAggFunction {

  private final SduAggFunction aggregateFunction;

  public SduCalciteAggFunction(
      String name,
      SduCalciteTypeFactory typeFactory,
      SduAggFunction aggFunction) {
    super(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        createReturnTypeInference(typeFactory, aggFunction),
        createOperandTypeInference(typeFactory, aggFunction),
        createOperandTypeChecker(typeFactory, aggFunction),
        // Do not need to provide a calcite aggregateFunction here. Flink aggregation function
        // will be generated when translating the calcite relnode to flink runtime execution plan
        null,
        false,
        aggFunction.isRequiresOver(),
        Optionality.FORBIDDEN,
        typeFactory
    );

    this.aggregateFunction = aggFunction;
  }

  @Override
  public boolean isDeterministic() {
    return aggregateFunction.isDeterministic();
  }

  private static SqlReturnTypeInference createReturnTypeInference(SduCalciteTypeFactory typeFactory, SduAggFunction aggFunction) {

    class SqlReturnTypeInferenceImpl implements SqlReturnTypeInference {

      private final SduCalciteTypeFactory typeFactory;
      private final SduAggFunction aggFunction;

      private SqlReturnTypeInferenceImpl(SduCalciteTypeFactory typeFactory, SduAggFunction aggFunction) {
        this.typeFactory = typeFactory;
        this.aggFunction = aggFunction;
      }

      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        Class<?>[] parameters = opBinding.collectOperandTypes().stream()
            .map(typeFactory::getJavaClass)
            .toArray(Class<?>[]::new);

        Method method = getAccumulateMethod(aggFunction.getUserDefinedFunction(), parameters);
        return typeFactory.createSqlType(method.getReturnType().getName());
      }
    }

    return new SqlReturnTypeInferenceImpl(typeFactory, aggFunction);
  }

  private static SqlOperandTypeInference createOperandTypeInference(
      SduCalciteTypeFactory typeFactory, SduAggFunction aggFunction) {
    throw new RuntimeException();
  }

  private static SqlOperandTypeChecker createOperandTypeChecker(SduCalciteTypeFactory typeFactory, SduAggFunction aggFunction) {
    throw new RuntimeException();
  }
}
