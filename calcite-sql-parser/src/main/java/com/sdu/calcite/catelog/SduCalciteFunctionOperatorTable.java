package com.sdu.calcite.catelog;

import com.sdu.sql.entry.SduAggFunction;
import com.sdu.sql.entry.SduFunction;
import com.sdu.sql.entry.SduScalarFunction;
import com.sdu.sql.entry.SduTableFunction;
import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.SduCalciteTypeFactory;
import com.sdu.sql.parse.UserDefinedFunctionUtils;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

public class SduCalciteFunctionOperatorTable implements SqlOperatorTable {

  private final SduCalciteFunctionCatalog functionCatalog;
  private final SduCalciteTypeFactory typeFactory;

  public SduCalciteFunctionOperatorTable(SduCalciteFunctionCatalog functionCatalog, SduCalciteTypeFactory typeFactory) {
    this.functionCatalog = functionCatalog;
    this.typeFactory = typeFactory;
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category,
      SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    if (!opName.isSimple()) {
      return;
    }

    if (isNotUserFunction(category)) {
      return;
    }

    final String name = opName.getSimple();
    functionCatalog.lookupFunction(name)
        .flatMap((SduFunction sduFunction) -> convertToSqlFunction(typeFactory, name, sduFunction))
        .ifPresent(operatorList::add);
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return null;
  }

  private boolean isNotUserFunction(SqlFunctionCategory category) {
    return category != null && !category.isUserDefinedNotSpecificFunction();
  }

  private static Optional<SqlFunction> convertToSqlFunction(SduCalciteTypeFactory typeFactory, String name, SduFunction function) {
    FunctionKind kind = function.getKind();
    switch (kind) {
      case TABLE:
        SduTableFunction tableFunction = (SduTableFunction) function;
        return Optional.of(UserDefinedFunctionUtils.convertTableSqlFunction(typeFactory, name, tableFunction));
      case SCALAR:
        SduScalarFunction scalarFunction = (SduScalarFunction) function;
        return Optional.of(UserDefinedFunctionUtils.convertScalarSqlFunction(typeFactory, name, scalarFunction));
      case AGGREGATE:
        SduAggFunction aggFunction = (SduAggFunction) function;
        return Optional.of(UserDefinedFunctionUtils.convertAggregateFunction(typeFactory, name, aggFunction));
      default:
        throw new RuntimeException("Unsupported sql function kind: " + kind);
    }
  }

}
