package com.sdu.calcite.plan;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/**
 * @author hanhan.zhang
 * */
public class FeatureSqlValidator extends SqlValidatorImpl {

    public FeatureSqlValidator(
            SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
        super(opTab, catalogReader, typeFactory, SqlConformanceEnum.DEFAULT);
    }

}
