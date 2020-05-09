package com.sdu.calcite.plan;

import org.apache.calcite.rel.type.RelDataType;

public interface SduSqlExprToRexConverterFactory {

  SduSqlExprToRexConverter create(RelDataType tableRowType);

}
