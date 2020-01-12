package com.sdu.calcite.sql;

import com.sdu.calcite.sql.ddl.SqlUseFunction;
import java.util.List;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

class XUserFunctionOperatorTable extends ReflectiveSqlOperatorTable {

   XUserFunctionOperatorTable(XTypeFactory typeFactory, List<SqlUseFunction> functions) {
    if (functions != null && functions.size() > 0) {
      for (SqlUseFunction function : functions) {
        // TODO: 2020-01-09
      }
    }
  }

}
