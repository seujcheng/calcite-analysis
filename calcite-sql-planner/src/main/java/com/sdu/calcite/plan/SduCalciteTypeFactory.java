package com.sdu.calcite.plan;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

public class SduCalciteTypeFactory extends JavaTypeFactoryImpl {

  public SduCalciteTypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  public RelDataType buildRelDataType(String[] fieldNames, String[] fieldTypes) {
    checkArgument(fieldNames != null);
    checkArgument(fieldTypes != null);
    checkState(fieldNames.length == fieldTypes.length);

    FieldInfoBuilder builder = builder();
    for (int i = 0; i < fieldNames.length; ++i) {
      builder.add(fieldNames[i], createSqlType(fieldTypes[i]));
    }
    return builder.build();
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
  public Type getJavaClass(RelDataType type) {
    switch (type.getSqlTypeName()) {
      /*
       * 基本类型必须返回Type, 原因:
       *
       * public class A {
       *
       *    public int eval(int a, int b);
       *
       * }
       *
       * 基于Java反射查找eval方法时:
       *
       *  若用 getMethod('eval', Integer.class, Integer.class) 则找不到方法的
       *
       *  若用 getMethod('eval', Integer.Type, Integer.Type) 则可以找到的
       *
       * */
      case BOOLEAN:
        return Boolean.TYPE;

      case TINYINT:
        return Byte.TYPE;

      case SMALLINT:
        return Short.TYPE;

      case INTEGER:
        return Integer.TYPE;

      case BIGINT:
        return Long.TYPE;

      case FLOAT:
        return Float.TYPE;

      case DOUBLE:
        return Double.TYPE;

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

      case NULL:
        return Void.class;

      case ANY:
        return Object.class;

      default:
        return super.getJavaClass(type);
    }
  }

  @Override
  public Charset getDefaultCharset() {
    // SQL中文处理
    return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
  }

}
