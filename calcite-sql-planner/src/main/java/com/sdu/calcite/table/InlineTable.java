package com.sdu.calcite.table;

import com.google.common.base.Preconditions;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author hanhan.zhang
 * */
abstract class InlineTable extends AbstractTable {

    private final String[] columnNames;
    private final SqlTypeName[] columnTypes;

    InlineTable(String[] columnNames, Class<?>[] columnTypes) {
        Preconditions.checkArgument(ArrayUtils.isNotEmpty(columnNames));
        Preconditions.checkArgument(ArrayUtils.isNotEmpty(columnTypes));
        Preconditions.checkState(columnNames.length == columnTypes.length);

        this.columnNames = columnNames;
        this.columnTypes = convertToSqlTypes(columnTypes);
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public SqlTypeName[] getColumnTypes() {
        return columnTypes;
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
