package com.sdu.calcite.table;

import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author hanhan.zhang
 * */
abstract class InlineTable extends AbstractTable {

    protected final String[] columnNames;
    protected final SqlTypeName[] columnTypes;

    InlineTable(String[] columnNames, Class<?>[] columnTypes) {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("Table column empty.");
        }
        if (columnTypes == null || columnTypes.length == 0) {
            throw new IllegalArgumentException("Table column type empty.");
        }
        if (columnNames.length != columnTypes.length) {
            throw new IllegalArgumentException("Table column and type length is not same.");
        }

        this.columnNames = columnNames;
        this.columnTypes = convertToSqlTypes(columnTypes);
    }

    private static SqlTypeName[] convertToSqlTypes(Class<?>[] columnTypes) {
        SqlTypeName[] sqlTypeNames = new SqlTypeName[columnTypes.length];

        for (int i = 0; i < columnTypes.length; ++i) {
            Class<?> clazz = columnTypes[i];
            if (clazz.equals(Integer.class)) {
                sqlTypeNames[i] = SqlTypeName.INTEGER;
            }
            else if (clazz.equals(Long.class)) {
                sqlTypeNames[i] = SqlTypeName.BIGINT;
            }
            else if (clazz.equals(Double.class)) {
                sqlTypeNames[i] = SqlTypeName.DOUBLE;
            }
            else if (clazz.equals(Float.class)) {
                sqlTypeNames[i] = SqlTypeName.FLOAT;
            }
            else if (clazz.equals(String.class)) {
                sqlTypeNames[i] = SqlTypeName.VARCHAR;
            }
            else if (clazz.equals(Character.class)) {
                sqlTypeNames[i] = SqlTypeName.CHAR;
            }
            else if (clazz.equals(Boolean.class)) {
                sqlTypeNames[i] = SqlTypeName.BOOLEAN;
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + clazz.getSimpleName());
            }
        }

        return sqlTypeNames;
    }

}
