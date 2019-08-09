package com.sdu.calcite.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class FeatureTable extends InlineTable {

    public FeatureTable(String[] columnNames, Class<?>[] columnTypes) {
        super(columnNames, columnTypes);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int i = 0; i < getColumnNames().length; ++i) {
            String name = getColumnNames()[i];
            SqlTypeName typeName = getColumnTypes()[i];
            builder.add(name, typeName);
        }
        return builder.build();
    }
}
