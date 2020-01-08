package com.sdu.calcite.plan;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

import java.util.ArrayList;
import java.util.List;

public class FunctionCatalog {

  private final List<SqlOperator> sqlFunctions = new ArrayList<>();

  private FunctionCatalog() {}

  public void registerSqlFunction(SqlFunction sqlFunction) {
    sqlFunctions.removeIf(func -> func.getName().equals(sqlFunction.getName()));
    sqlFunctions.add(sqlFunction);
  }

  public SqlOperatorTable getSqlOperatorTable() {
    return ChainedSqlOperatorTable.of(
        new ListSqlOperatorTable(sqlFunctions)
    );
  }

  // -----------------------------------------------------------------------
  public static FunctionCatalog withBuiltIns() {
    return new FunctionCatalog();
  }

}
