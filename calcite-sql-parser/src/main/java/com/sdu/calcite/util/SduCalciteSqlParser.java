package com.sdu.calcite.util;

import com.sdu.calcite.entry.SduFunction;
import com.sdu.calcite.entry.SduInsert;
import com.sdu.calcite.entry.SduSqlStatement;
import com.sdu.calcite.entry.SduTable;
import com.sdu.calcite.parser.SduCalciteSqlParserFactory;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import com.sdu.calcite.sql.parser.SduSqlParserImpl;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNodeList;

public class SduCalciteSqlParser {

  private static SqlNodeList parse(String sql) throws Exception {
    StringReader reader = new StringReader(sql);
    SduSqlParserImpl parser = new SduCalciteSqlParserFactory().getParser(reader);
    return parser.parseSqlStmtList();
  }

  private static List<SduTable> definedTables(SqlNodeList sqlNodes) {
    if (sqlNodes == null) {
      return Collections.emptyList();
    }
    return sqlNodes.getList()
        .stream()
        .filter(sqlNode -> sqlNode instanceof SqlCreateTable)
        .map(SduTable::fromSqlCreateTable)
        .collect(Collectors.toList());
  }

  private static List<SduFunction> definedFunctions(SqlNodeList sqlNodes) {
    if (sqlNodes == null) {
      return Collections.emptyList();
    }
    return sqlNodes.getList()
        .stream()
        .filter(sqlNode -> sqlNode instanceof SqlCreateFunction)
        .map(SduFunction::fromSqlCreateFunction)
        .collect(Collectors.toList());
  }

  private static List<SduInsert> definedInserts(SqlNodeList sqlNodes) {
    if (sqlNodes == null) {
      return Collections.emptyList();
    }
    return sqlNodes.getList()
        .stream()
        .filter(sqlNode -> sqlNode instanceof SqlInsert)
        .map(SduInsert::fromSqlInsert)
        .collect(Collectors.toList());
  }

  public static SduSqlStatement userDefinedSqlStatement(String sql) throws Exception {
    SqlNodeList sqlNodes = parse(sql);
    // 表
    List<SduTable> sduTables = definedTables(sqlNodes);
    // 函数
    List<SduFunction> sduFunctions = definedFunctions(sqlNodes);
    //
    List<SduInsert> inserts = definedInserts(sqlNodes);
    return SduSqlStatement.of(sduTables, sduFunctions, inserts);
  }

}
