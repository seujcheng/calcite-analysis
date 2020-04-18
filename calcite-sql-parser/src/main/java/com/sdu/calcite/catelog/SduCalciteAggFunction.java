package com.sdu.calcite.catelog;

import com.sdu.calcite.entry.SduAggregateFunction;
import com.sdu.calcite.types.SduTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;

public class SduCalciteAggFunction extends SqlUserDefinedAggFunction {

  private final SduAggregateFunction aggregateFunction;

  public SduCalciteAggFunction(
      String name,
      SduTypeFactory typeFactory,
      SduAggregateFunction aggFunction) {
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

  private static SqlReturnTypeInference createReturnTypeInference(SduTypeFactory typeFactory, SduAggregateFunction aggFunction) {
    throw new RuntimeException();
  }

  private static SqlOperandTypeInference createOperandTypeInference(SduTypeFactory typeFactory, SduAggregateFunction aggFunction) {
    throw new RuntimeException();
  }

  private static SqlOperandTypeChecker createOperandTypeChecker(SduTypeFactory typeFactory, SduAggregateFunction aggFunction) {
    throw new RuntimeException();
  }
}
