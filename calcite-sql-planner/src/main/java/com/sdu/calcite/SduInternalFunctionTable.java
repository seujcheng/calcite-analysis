package com.sdu.calcite;

import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL_INTERVAL;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME;
import static org.apache.calcite.sql.type.OperandTypes.DATETIME_INTERVAL_TIME;

import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class SduInternalFunctionTable extends ReflectiveSqlOperatorTable {

  // 翻转窗口
  private static final SqlGroupedWindowFunction TUMBLE = new SqlGroupedWindowFunction(
      SqlKind.TUMBLE, null, OperandTypes.or(DATETIME_INTERVAL, DATETIME_INTERVAL_TIME)) {
    @Override
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
      return Lists.newArrayList(TUMBLE_START, TUMBLE_END);
    }
  };
  private static final SqlGroupedWindowFunction TUMBLE_START = TUMBLE.auxiliary(SqlKind.TUMBLE_START);
  private static final SqlGroupedWindowFunction TUMBLE_END = TUMBLE.auxiliary(SqlKind.TUMBLE_END);

  // 滑动窗口
  private static final SqlGroupedWindowFunction HOP = new SqlGroupedWindowFunction(
      SqlKind.HOP, null, OperandTypes.or(DATETIME_INTERVAL_INTERVAL, DATETIME_INTERVAL_INTERVAL_TIME)) {
    @Override
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
      return Lists.newArrayList(HOP_START, HOP_END);
    }
  };
  private static final SqlGroupedWindowFunction HOP_START = HOP.auxiliary(SqlKind.HOP_START);
  private static final SqlGroupedWindowFunction HOP_END = HOP.auxiliary(SqlKind.HOP_END);

  // 会话窗口
  private static final SqlGroupedWindowFunction SESSION = new SqlGroupedWindowFunction(
      SqlKind.SESSION, null, OperandTypes.or(DATETIME_INTERVAL, DATETIME_INTERVAL_TIME)) {

    @Override
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
      return Lists.newArrayList(SESSION_START, SESSION_END);
    }
  };
  private static final SqlGroupedWindowFunction SESSION_START = SESSION.auxiliary(SqlKind.SESSION_START);
  private static final SqlGroupedWindowFunction SESSION_END = SESSION.auxiliary(SqlKind.SESSION_END);

  private static final List<SqlOperator> builtInSqlOperators;

  static {
    builtInSqlOperators = new LinkedList<>();

    // SET OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.UNION);
    builtInSqlOperators.add(SqlStdOperatorTable.UNION_ALL);
    builtInSqlOperators.add(SqlStdOperatorTable.EXCEPT);
    builtInSqlOperators.add(SqlStdOperatorTable.EXCEPT_ALL);
    builtInSqlOperators.add(SqlStdOperatorTable.INTERSECT);
    builtInSqlOperators.add(SqlStdOperatorTable.INTERSECT_ALL);

    // BINARY OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.AND);
    builtInSqlOperators.add(SqlStdOperatorTable.AS);
    builtInSqlOperators.add(SqlStdOperatorTable.CONCAT);
    builtInSqlOperators.add(SqlStdOperatorTable.DIVIDE);
    builtInSqlOperators.add(SqlStdOperatorTable.DIVIDE_INTEGER);
    builtInSqlOperators.add(SqlStdOperatorTable.DOT);
    builtInSqlOperators.add(SqlStdOperatorTable.EQUALS);
    builtInSqlOperators.add(SqlStdOperatorTable.GREATER_THAN);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_DISTINCT_FROM);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);
    builtInSqlOperators.add(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    builtInSqlOperators.add(SqlStdOperatorTable.LESS_THAN);
    builtInSqlOperators.add(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
    builtInSqlOperators.add(SqlStdOperatorTable.MINUS);
    builtInSqlOperators.add(SqlStdOperatorTable.MULTIPLY);
    builtInSqlOperators.add(SqlStdOperatorTable.NOT_EQUALS);
    builtInSqlOperators.add(SqlStdOperatorTable.OR);
    builtInSqlOperators.add(SqlStdOperatorTable.PLUS);
    builtInSqlOperators.add(SqlStdOperatorTable.DATETIME_PLUS);

    // POSTFIX OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.DESC);
    builtInSqlOperators.add(SqlStdOperatorTable.NULLS_FIRST);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_NOT_NULL);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_NULL);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_NOT_TRUE);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_TRUE);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_NOT_FALSE);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_FALSE);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_NOT_UNKNOWN);
    builtInSqlOperators.add(SqlStdOperatorTable.IS_UNKNOWN);

    // PREFIX OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.NOT);
    builtInSqlOperators.add(SqlStdOperatorTable.UNARY_MINUS);
    builtInSqlOperators.add(SqlStdOperatorTable.UNARY_PLUS);

    // GROUPING FUNCTIONS
    builtInSqlOperators.add(SqlStdOperatorTable.GROUP_ID);
    builtInSqlOperators.add(SqlStdOperatorTable.GROUPING);
    builtInSqlOperators.add(SqlStdOperatorTable.GROUPING_ID);

    // AGGREGATE OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.SUM);
    builtInSqlOperators.add(SqlStdOperatorTable.SUM0);
    builtInSqlOperators.add(SqlStdOperatorTable.COUNT);
    builtInSqlOperators.add(SqlStdOperatorTable.COLLECT);
    builtInSqlOperators.add(SqlStdOperatorTable.MIN);
    builtInSqlOperators.add(SqlStdOperatorTable.MAX);
    builtInSqlOperators.add(SqlStdOperatorTable.AVG);
    builtInSqlOperators.add(SqlStdOperatorTable.STDDEV_POP);
    builtInSqlOperators.add(SqlStdOperatorTable.STDDEV_SAMP);
    builtInSqlOperators.add(SqlStdOperatorTable.VAR_POP);
    builtInSqlOperators.add(SqlStdOperatorTable.VAR_SAMP);

    // ARRAY OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);
    builtInSqlOperators.add(SqlStdOperatorTable.ELEMENT);

    // MAP OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);

    // ARRAY MAP SHARED OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.ITEM);
    builtInSqlOperators.add(SqlStdOperatorTable.CARDINALITY);

    // SPECIAL OPERATORS
    builtInSqlOperators.add(SqlStdOperatorTable.ROW);
    builtInSqlOperators.add(SqlStdOperatorTable.OVERLAPS);
    builtInSqlOperators.add(SqlStdOperatorTable.LITERAL_CHAIN);
    builtInSqlOperators.add(SqlStdOperatorTable.BETWEEN);
    builtInSqlOperators.add(SqlStdOperatorTable.SYMMETRIC_BETWEEN);
    builtInSqlOperators.add(SqlStdOperatorTable.NOT_BETWEEN);
    builtInSqlOperators.add(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN);
    builtInSqlOperators.add(SqlStdOperatorTable.NOT_LIKE);
    builtInSqlOperators.add(SqlStdOperatorTable.LIKE);
    builtInSqlOperators.add(SqlStdOperatorTable.NOT_SIMILAR_TO);
    builtInSqlOperators.add(SqlStdOperatorTable.SIMILAR_TO);
    builtInSqlOperators.add(SqlStdOperatorTable.CASE);
    builtInSqlOperators.add(SqlStdOperatorTable.REINTERPRET);
    builtInSqlOperators.add(SqlStdOperatorTable.EXTRACT);
    builtInSqlOperators.add(SqlStdOperatorTable.IN);

    // FUNCTIONS
    builtInSqlOperators.add(SqlStdOperatorTable.SUBSTRING);
    builtInSqlOperators.add(SqlStdOperatorTable.OVERLAY);
    builtInSqlOperators.add(SqlStdOperatorTable.TRIM);
    builtInSqlOperators.add(SqlStdOperatorTable.POSITION);
    builtInSqlOperators.add(SqlStdOperatorTable.CHAR_LENGTH);
    builtInSqlOperators.add(SqlStdOperatorTable.CHARACTER_LENGTH);
    builtInSqlOperators.add(SqlStdOperatorTable.UPPER);
    builtInSqlOperators.add(SqlStdOperatorTable.LOWER);
    builtInSqlOperators.add(SqlStdOperatorTable.INITCAP);
    builtInSqlOperators.add(SqlStdOperatorTable.POWER);
    builtInSqlOperators.add(SqlStdOperatorTable.SQRT);
    builtInSqlOperators.add(SqlStdOperatorTable.MOD);
    builtInSqlOperators.add(SqlStdOperatorTable.LN);
    builtInSqlOperators.add(SqlStdOperatorTable.LOG10);
    builtInSqlOperators.add(SqlStdOperatorTable.ABS);
    builtInSqlOperators.add(SqlStdOperatorTable.EXP);
    builtInSqlOperators.add(SqlStdOperatorTable.NULLIF);
    builtInSqlOperators.add(SqlStdOperatorTable.COALESCE);
    builtInSqlOperators.add(SqlStdOperatorTable.FLOOR);
    builtInSqlOperators.add(SqlStdOperatorTable.CEIL);
    builtInSqlOperators.add(SqlStdOperatorTable.LOCALTIME);
    builtInSqlOperators.add(SqlStdOperatorTable.LOCALTIMESTAMP);
    builtInSqlOperators.add(SqlStdOperatorTable.CURRENT_TIME);
    builtInSqlOperators.add(SqlStdOperatorTable.CURRENT_TIMESTAMP);
    builtInSqlOperators.add(SqlStdOperatorTable.CURRENT_DATE);
    builtInSqlOperators.add(SqlStdOperatorTable.CAST);
    builtInSqlOperators.add(SqlStdOperatorTable.SCALAR_QUERY);
    builtInSqlOperators.add(SqlStdOperatorTable.EXISTS);
    builtInSqlOperators.add(SqlStdOperatorTable.SIN);
    builtInSqlOperators.add(SqlStdOperatorTable.COS);
    builtInSqlOperators.add(SqlStdOperatorTable.TAN);
    builtInSqlOperators.add(SqlStdOperatorTable.COT);
    builtInSqlOperators.add(SqlStdOperatorTable.ASIN);
    builtInSqlOperators.add(SqlStdOperatorTable.ACOS);
    builtInSqlOperators.add(SqlStdOperatorTable.ATAN);
    builtInSqlOperators.add(SqlStdOperatorTable.ATAN2);
    builtInSqlOperators.add(SqlStdOperatorTable.DEGREES);
    builtInSqlOperators.add(SqlStdOperatorTable.RADIANS);
    builtInSqlOperators.add(SqlStdOperatorTable.SIGN);
    builtInSqlOperators.add(SqlStdOperatorTable.ROUND);
    builtInSqlOperators.add(SqlStdOperatorTable.PI);
    builtInSqlOperators.add(SqlStdOperatorTable.RAND);
    builtInSqlOperators.add(SqlStdOperatorTable.RAND_INTEGER);
    builtInSqlOperators.add(SqlStdOperatorTable.REPLACE);
    builtInSqlOperators.add(SqlStdOperatorTable.TRUNCATE);

    // TIME FUNCTIONS
    builtInSqlOperators.add(SqlStdOperatorTable.YEAR);
    builtInSqlOperators.add(SqlStdOperatorTable.QUARTER);
    builtInSqlOperators.add(SqlStdOperatorTable.MONTH);
    builtInSqlOperators.add(SqlStdOperatorTable.WEEK);
    builtInSqlOperators.add(SqlStdOperatorTable.HOUR);
    builtInSqlOperators.add(SqlStdOperatorTable.MINUTE);
    builtInSqlOperators.add(SqlStdOperatorTable.SECOND);
    builtInSqlOperators.add(SqlStdOperatorTable.DAYOFYEAR);
    builtInSqlOperators.add(SqlStdOperatorTable.DAYOFMONTH);
    builtInSqlOperators.add(SqlStdOperatorTable.DAYOFWEEK);
    builtInSqlOperators.add(SqlStdOperatorTable.TIMESTAMP_ADD);
    builtInSqlOperators.add(SqlStdOperatorTable.TIMESTAMP_DIFF);

    // MATCH_RECOGNIZE
    builtInSqlOperators.add(SqlStdOperatorTable.FIRST);
    builtInSqlOperators.add(SqlStdOperatorTable.LAST);
    builtInSqlOperators.add(SqlStdOperatorTable.PREV);
    builtInSqlOperators.add(SqlStdOperatorTable.FINAL);
    builtInSqlOperators.add(SqlStdOperatorTable.RUNNING);

    // WINDOW
    builtInSqlOperators.add(SduInternalFunctionTable.TUMBLE);
    builtInSqlOperators.add(SduInternalFunctionTable.TUMBLE_START);
    builtInSqlOperators.add(SduInternalFunctionTable.TUMBLE_END);
    builtInSqlOperators.add(SduInternalFunctionTable.HOP);
    builtInSqlOperators.add(SduInternalFunctionTable.HOP_START);
    builtInSqlOperators.add(SduInternalFunctionTable.HOP_END);
    builtInSqlOperators.add(SduInternalFunctionTable.SESSION);
    builtInSqlOperators.add(SduInternalFunctionTable.SESSION_START);
    builtInSqlOperators.add(SduInternalFunctionTable.SESSION_END);

    // OVER WINDOW
    builtInSqlOperators.add(SqlStdOperatorTable.RANK);
    builtInSqlOperators.add(SqlStdOperatorTable.ROW_NUMBER);

  }

  public SduInternalFunctionTable() {
    builtInSqlOperators.forEach(this::register);
  }

}
