package com.sdu.calcite.util;

import com.sdu.calcite.entry.SduFunction;
import com.sdu.calcite.types.SduTypeFactory;
import java.util.List;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

class SduFunctionOperatorTable extends ReflectiveSqlOperatorTable {

  SduFunctionOperatorTable(SduTypeFactory typeFactory, List<SduFunction> functions) {
    if (functions != null && functions.size() > 0) {
    }
  }

}
