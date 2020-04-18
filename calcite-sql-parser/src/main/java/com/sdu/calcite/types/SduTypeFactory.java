package com.sdu.calcite.types;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

public class SduTypeFactory extends JavaTypeFactoryImpl {

  RelDataType createTypeFromClass(Class<?> cls) {
    return createSqlType(cls.getName());
  }

  public RelDataType createSqlType(String typeName) {
    switch (typeName) {
      // 基本类型
      case "BOOLEAN":
      case "java.lang.Boolean":
        return createSqlType(SqlTypeName.BOOLEAN);

      case "TINYINT":
      case "java.lang.Byte":
        return createSqlType(SqlTypeName.TINYINT);

      case "SMALLINT":
      case "java.lang.Short":
        return createSqlType(SqlTypeName.SMALLINT);

      case "INTEGER":
      case "java.lang.Integer":
        return createSqlType(SqlTypeName.INTEGER);

      case "BIGINT":
      case "java.lang.Long":
        return createSqlType(SqlTypeName.BIGINT);

      case "FLOAT":
      case "java.lang.Float":
        return createSqlType(SqlTypeName.FLOAT);

      case "DOUBLE":
      case "java.lang.Double":
        return createSqlType(SqlTypeName.DOUBLE);

      case "DECIMAL":
      case "java.math.BigDecimal":
        return createSqlType(SqlTypeName.DECIMAL);

      case "CHAR":
      case "VARCHAR":
      case "java.lang.String":
        return createSqlType(SqlTypeName.VARCHAR);

      case "DATE":
      case "java.sql.Date":
        return createSqlType(SqlTypeName.DATE);

      case "TIME":
      case "java.sql.Time":
        return createSqlType(SqlTypeName.TIME);

      case "TIMESTAMP":
      case "java.sql.Timestamp":
        return createSqlType(SqlTypeName.TIMESTAMP);

      // TODO: 复合类型, 尚未对齐
      case "MAP":
      case "java.util.Map":
        return createSqlType(SqlTypeName.MAP);

      case "ROW":
      case "org.apache.calcite.rel.type.RelRecordType":
        return createSqlType(SqlTypeName.ROW);

      case "ANY":
      case "java.lang.Object":
        return createSqlType(SqlTypeName.ANY);

      default:
        throw new IllegalArgumentException("Unsupported data type: " + typeName);
    }
  }

  @Override
  public Class<?> getJavaClass(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Boolean.class;

      case TINYINT:
        return Byte.class;

      case SMALLINT:
        return Short.class;

      case INTEGER:
        return Integer.class;

      case BIGINT:
        return Long.class;

      case FLOAT:
        return Float.class;

      case DOUBLE:
        return Double.class;

      case DECIMAL:
        return BigDecimal.class;

      case CHAR:
      case VARCHAR:
        return String.class;

      case DATE:
        return Date.class;

      case TIME:
        return Time.class;

      case TIMESTAMP:
        return Timestamp.class;

      // TODO: 复合类型, 尚未对齐
      case MAP:
        return Map.class;

      case ANY:
        return Object.class;

      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  @Override
  public Charset getDefaultCharset() {
    // SQL中文处理
    return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
  }

}
