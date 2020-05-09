package com.sdu.calcite.plan;

import com.sdu.calcite.api.SduTableConfig;
import org.apache.calcite.plan.Context;

public interface SduContext extends Context {

  SduTableConfig getTableConfig();

  SduSqlExprToRexConverterFactory getSqlExprToRexConverterFactory();

}
