package com.sdu.calcite;

import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL_INTERVAL;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL_TIME;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class SduInternalFunctionTable extends ReflectiveSqlOperatorTable {

  private static SduInternalFunctionTable INSTANCE;

  // 翻转窗口
  public static final SqlGroupedWindowFunction TUMBLE = new SqlGroupedWindowFunction(
      SqlKind.TUMBLE, null, OperandTypes.or(DATETIME_INTERVAL, DATETIME_INTERVAL_TIME)) {
    @Override
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
      return Lists.newArrayList(TUMBLE_START, TUMBLE_END);
    }
  };
  public static final SqlGroupedWindowFunction TUMBLE_START = TUMBLE.auxiliary(SqlKind.TUMBLE_START);
  public static final SqlGroupedWindowFunction TUMBLE_END = TUMBLE.auxiliary(SqlKind.TUMBLE_END);

  // 滑动窗口
  public static final SqlGroupedWindowFunction HOP = new SqlGroupedWindowFunction(
      SqlKind.HOP, null, OperandTypes.or(DATETIME_INTERVAL_INTERVAL, DATETIME_INTERVAL_INTERVAL_TIME)) {
    @Override
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
      return Lists.newArrayList(HOP_START, HOP_END);
    }
  };
  public static final SqlGroupedWindowFunction HOP_START = HOP.auxiliary(SqlKind.HOP_START);
  public static final SqlGroupedWindowFunction HOP_END = HOP.auxiliary(SqlKind.HOP_END);

  // 会话窗口
  public static final SqlGroupedWindowFunction SESSION = new SqlGroupedWindowFunction(
      SqlKind.SESSION, null, OperandTypes.or(DATETIME_INTERVAL, DATETIME_INTERVAL_TIME)) {

    @Override
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
      return Lists.newArrayList(SESSION_START, SESSION_END);
    }
  };
  public static final SqlGroupedWindowFunction SESSION_START = SESSION.auxiliary(SqlKind.SESSION_START);
  public static final SqlGroupedWindowFunction SESSION_END = SESSION.auxiliary(SqlKind.SESSION_END);


  // SET OPERATORS
  public static final SqlOperator UNION = SqlStdOperatorTable.UNION;
  public static final SqlOperator UNION_ALL = SqlStdOperatorTable.UNION_ALL;
  public static final SqlOperator EXCEPT = SqlStdOperatorTable.EXCEPT;
  public static final SqlOperator EXCEPT_ALL = SqlStdOperatorTable.EXCEPT_ALL;
  public static final SqlOperator INTERSECT = SqlStdOperatorTable.INTERSECT;
  public static final SqlOperator INTERSECT_ALL = SqlStdOperatorTable.INTERSECT_ALL;

  // BINARY OPERATORS
  public static final SqlOperator AND = SqlStdOperatorTable.AND;
  public static final SqlOperator AS = SqlStdOperatorTable.AS;
  public static final SqlOperator CONCAT = SqlStdOperatorTable.CONCAT;
  public static final SqlOperator DIVIDE = SqlStdOperatorTable.DIVIDE;
  public static final SqlOperator DIVIDE_INTEGER = SqlStdOperatorTable.DIVIDE_INTEGER;
  public static final SqlOperator DOT = SqlStdOperatorTable.DOT;
  public static final SqlOperator EQUALS = SqlStdOperatorTable.EQUALS;
  public static final SqlOperator GREATER_THAN = SqlStdOperatorTable.GREATER_THAN;
  public static final SqlOperator IS_DISTINCT_FROM = SqlStdOperatorTable.IS_DISTINCT_FROM;
  public static final SqlOperator IS_NOT_DISTINCT_FROM = SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
  public static final SqlOperator GREATER_THAN_OR_EQUAL = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
  public static final SqlOperator LESS_THAN = SqlStdOperatorTable.LESS_THAN;
  public static final SqlOperator LESS_THAN_OR_EQUAL = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
  public static final SqlOperator MINUS = SqlStdOperatorTable.MINUS;
  public static final SqlOperator MULTIPLY = SqlStdOperatorTable.MULTIPLY;
  public static final SqlOperator NOT_EQUALS = SqlStdOperatorTable.NOT_EQUALS;
  public static final SqlOperator OR = SqlStdOperatorTable.OR;
  public static final SqlOperator PLUS = SqlStdOperatorTable.PLUS;
  public static final SqlOperator DATETIME_PLUS = SqlStdOperatorTable.DATETIME_PLUS;

  // POSTFIX OPERATORS
  public static final SqlOperator DESC = SqlStdOperatorTable.DESC;
  public static final SqlOperator NULLS_FIRST = SqlStdOperatorTable.NULLS_FIRST;
  public static final SqlOperator IS_NOT_NULL = SqlStdOperatorTable.IS_NOT_NULL;
  public static final SqlOperator IS_NULL = SqlStdOperatorTable.IS_NULL;
  public static final SqlOperator IS_NOT_TRUE = SqlStdOperatorTable.IS_NOT_TRUE;
  public static final SqlOperator IS_TRUE = SqlStdOperatorTable.IS_TRUE;
  public static final SqlOperator IS_NOT_FALSE = SqlStdOperatorTable.IS_NOT_FALSE;
  public static final SqlOperator IS_FALSE = SqlStdOperatorTable.IS_FALSE;
  public static final SqlOperator IS_NOT_UNKNOWN = SqlStdOperatorTable.IS_NOT_UNKNOWN;
  public static final SqlOperator IS_UNKNOWN = SqlStdOperatorTable.IS_UNKNOWN;

  // PREFIX OPERATORS
  public static final SqlOperator NOT = SqlStdOperatorTable.NOT;
  public static final SqlOperator UNARY_MINUS = SqlStdOperatorTable.UNARY_MINUS;
  public static final SqlOperator UNARY_PLUS = SqlStdOperatorTable.UNARY_PLUS;

  // GROUPING FUNCTIONS
  public static final SqlOperator GROUP_ID = SqlStdOperatorTable.GROUP_ID;
  public static final SqlOperator GROUPING = SqlStdOperatorTable.GROUPING;
  public static final SqlOperator GROUPING_ID = SqlStdOperatorTable.GROUPING_ID;

  // AGGREGATE OPERATORS
  public static final SqlOperator SUM = SqlStdOperatorTable.SUM;
  public static final SqlOperator SUM0 = SqlStdOperatorTable.SUM0;
  public static final SqlOperator COUNT = SqlStdOperatorTable.COUNT;
  public static final SqlOperator COLLECT = SqlStdOperatorTable.COLLECT;
  public static final SqlOperator MIN = SqlStdOperatorTable.MIN;
  public static final SqlOperator MAX = SqlStdOperatorTable.MAX;
  public static final SqlOperator AVG = SqlStdOperatorTable.AVG;
  public static final SqlOperator STDDEV_POP = SqlStdOperatorTable.STDDEV_POP;
  public static final SqlOperator STDDEV_SAMP = SqlStdOperatorTable.STDDEV_SAMP;
  public static final SqlOperator VAR_POP = SqlStdOperatorTable.VAR_POP;
  public static final SqlOperator VAR_SAMP = SqlStdOperatorTable.VAR_SAMP;

  // ARRAY OPERATORS
  public static final SqlOperator ARRAY_VALUE_CONSTRUCTOR = SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
  public static final SqlOperator ELEMENT = SqlStdOperatorTable.ELEMENT;

  // MAP OPERATORS
  public static final SqlOperator MAP_VALUE_CONSTRUCTOR = SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;

  // ARRAY MAP SHARED OPERATORS
  public static final SqlOperator ITEM = SqlStdOperatorTable.ITEM;
  public static final SqlOperator CARDINALITY = SqlStdOperatorTable.CARDINALITY;

  // SPECIAL OPERATORS
  public static final SqlOperator ROW = SqlStdOperatorTable.ROW;
  public static final SqlOperator OVERLAPS = SqlStdOperatorTable.OVERLAPS;
  public static final SqlOperator LITERAL_CHAIN = SqlStdOperatorTable.LITERAL_CHAIN;
  public static final SqlOperator BETWEEN = SqlStdOperatorTable.BETWEEN;
  public static final SqlOperator SYMMETRIC_BETWEEN = SqlStdOperatorTable.SYMMETRIC_BETWEEN;
  public static final SqlOperator NOT_BETWEEN = SqlStdOperatorTable.NOT_BETWEEN;
  public static final SqlOperator SYMMETRIC_NOT_BETWEEN = SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN;
  public static final SqlOperator NOT_LIKE = SqlStdOperatorTable.NOT_LIKE;
  public static final SqlOperator LIKE = SqlStdOperatorTable.LIKE;
  public static final SqlOperator NOT_SIMILAR_TO = SqlStdOperatorTable.NOT_SIMILAR_TO;
  public static final SqlOperator SIMILAR_TO = SqlStdOperatorTable.SIMILAR_TO;
  public static final SqlOperator CASE = SqlStdOperatorTable.CASE;
  public static final SqlOperator REINTERPRET = SqlStdOperatorTable.REINTERPRET;
  public static final SqlOperator EXTRACT = SqlStdOperatorTable.EXTRACT;
  public static final SqlOperator IN = SqlStdOperatorTable.IN;

  // FUNCTIONS
  public static final SqlOperator SUBSTRING = SqlStdOperatorTable.SUBSTRING;
  public static final SqlOperator OVERLAY = SqlStdOperatorTable.OVERLAY;
  public static final SqlOperator TRIM = SqlStdOperatorTable.TRIM;
  public static final SqlOperator POSITION = SqlStdOperatorTable.POSITION;
  public static final SqlOperator CHAR_LENGTH = SqlStdOperatorTable.CHAR_LENGTH;
  public static final SqlOperator CHARACTER_LENGTH = SqlStdOperatorTable.CHARACTER_LENGTH;
  public static final SqlOperator UPPER = SqlStdOperatorTable.UPPER;
  public static final SqlOperator LOWER = SqlStdOperatorTable.LOWER;
  public static final SqlOperator INITCAP = SqlStdOperatorTable.INITCAP;
  public static final SqlOperator POWER = SqlStdOperatorTable.POWER;
  public static final SqlOperator SQRT = SqlStdOperatorTable.SQRT;
  public static final SqlOperator MOD = SqlStdOperatorTable.MOD;
  public static final SqlOperator LN = SqlStdOperatorTable.LN;
  public static final SqlOperator LOG10 = SqlStdOperatorTable.LOG10;
  public static final SqlOperator ABS = SqlStdOperatorTable.ABS;
  public static final SqlOperator EXP = SqlStdOperatorTable.EXP;
  public static final SqlOperator NULLIF = SqlStdOperatorTable.NULLIF;
  public static final SqlOperator COALESCE = SqlStdOperatorTable.COALESCE;
  public static final SqlOperator FLOOR = SqlStdOperatorTable.FLOOR;
  public static final SqlOperator CEIL = SqlStdOperatorTable.CEIL;
  public static final SqlOperator LOCALTIME = SqlStdOperatorTable.LOCALTIME;
  public static final SqlOperator LOCALTIMESTAMP = SqlStdOperatorTable.LOCALTIMESTAMP;
  public static final SqlOperator CURRENT_TIME = SqlStdOperatorTable.CURRENT_TIME;
  public static final SqlOperator CURRENT_TIMESTAMP = SqlStdOperatorTable.CURRENT_TIMESTAMP;
  public static final SqlOperator CURRENT_DATE = SqlStdOperatorTable.CURRENT_DATE;
  public static final SqlOperator CAST = SqlStdOperatorTable.CAST;
  public static final SqlOperator SCALAR_QUERY = SqlStdOperatorTable.SCALAR_QUERY;
  public static final SqlOperator EXISTS = SqlStdOperatorTable.EXISTS;
  public static final SqlOperator SIN = SqlStdOperatorTable.SIN;
  public static final SqlOperator COS = SqlStdOperatorTable.COS;
  public static final SqlOperator TAN = SqlStdOperatorTable.TAN;
  public static final SqlOperator COT = SqlStdOperatorTable.COT;
  public static final SqlOperator ASIN = SqlStdOperatorTable.ASIN;
  public static final SqlOperator ACOS = SqlStdOperatorTable.ACOS;
  public static final SqlOperator ATAN = SqlStdOperatorTable.ATAN;
  public static final SqlOperator ATAN2 = SqlStdOperatorTable.ATAN2;
  public static final SqlOperator DEGREES = SqlStdOperatorTable.DEGREES;
  public static final SqlOperator RADIANS = SqlStdOperatorTable.RADIANS;
  public static final SqlOperator SIGN = SqlStdOperatorTable.SIGN;
  public static final SqlOperator ROUND = SqlStdOperatorTable.ROUND;
  public static final SqlOperator PI = SqlStdOperatorTable.PI;
  public static final SqlOperator RAND = SqlStdOperatorTable.RAND;
  public static final SqlOperator RAND_INTEGER = SqlStdOperatorTable.RAND_INTEGER;
  public static final SqlOperator REPLACE = SqlStdOperatorTable.REPLACE;
  public static final SqlOperator TRUNCATE = SqlStdOperatorTable.TRUNCATE;

  // TIME FUNCTIONS
  public static final SqlOperator YEAR = SqlStdOperatorTable.YEAR;
  public static final SqlOperator QUARTER = SqlStdOperatorTable.QUARTER;
  public static final SqlOperator MONTH = SqlStdOperatorTable.MONTH;
  public static final SqlOperator WEEK = SqlStdOperatorTable.WEEK;
  public static final SqlOperator HOUR = SqlStdOperatorTable.HOUR;
  public static final SqlOperator MINUTE = SqlStdOperatorTable.MINUTE;
  public static final SqlOperator SECOND = SqlStdOperatorTable.SECOND;
  public static final SqlOperator DAYOFYEAR = SqlStdOperatorTable.DAYOFYEAR;
  public static final SqlOperator DAYOFMONTH = SqlStdOperatorTable.DAYOFMONTH;
  public static final SqlOperator DAYOFWEEK = SqlStdOperatorTable.DAYOFWEEK;
  public static final SqlOperator TIMESTAMP_ADD = SqlStdOperatorTable.TIMESTAMP_ADD;
  public static final SqlOperator TIMESTAMP_DIFF = SqlStdOperatorTable.TIMESTAMP_DIFF;

  // MATCH_RECOGNIZE
  public static final SqlOperator FIRST = SqlStdOperatorTable.FIRST;
  public static final SqlOperator LAST = SqlStdOperatorTable.LAST;
  public static final SqlOperator PREV = SqlStdOperatorTable.PREV;
  public static final SqlOperator FINAL = SqlStdOperatorTable.FINAL;
  public static final SqlOperator RUNNING = SqlStdOperatorTable.RUNNING;


  // OVER WINDOW
  public static final SqlOperator RANK = SqlStdOperatorTable.RANK;
  public static final SqlOperator ROW_NUMBER = SqlStdOperatorTable.ROW_NUMBER;
    

  public static synchronized SduInternalFunctionTable instance() {
    if (INSTANCE == null) {
      INSTANCE = new SduInternalFunctionTable();
      INSTANCE.init();
    }
    return INSTANCE;
  }

  private SduInternalFunctionTable() {
    
  }

}
