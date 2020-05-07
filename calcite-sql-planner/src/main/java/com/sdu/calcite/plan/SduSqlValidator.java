package com.sdu.calcite.plan;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

class SduSqlValidator extends SqlValidatorImpl {

  SduSqlValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      JavaTypeFactory typeFactory) {
    super(opTab, catalogReader, typeFactory, SqlConformanceEnum.DEFAULT);
  }

}
