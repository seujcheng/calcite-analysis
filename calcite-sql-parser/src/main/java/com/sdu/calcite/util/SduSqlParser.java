package com.sdu.calcite.util;

import com.sdu.calcite.entry.SduFunction;
import com.sdu.calcite.entry.SduSqlStatement;
import com.sdu.calcite.entry.SduTable;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import com.sdu.calcite.sql.parser.SduSqlParserImpl;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;

public class SduSqlParser {

  private static SqlNodeList parse(String sql) throws Exception {
    SqlParser.Config config = SqlParser.configBuilder()
        .setLex(Lex.JAVA)
        .setQuotedCasing(Lex.JAVA.quotedCasing)
        .setUnquotedCasing(Lex.JAVA.unquotedCasing)
        .build();

    SduSqlParserImpl parser = new SduSqlParserImpl(new StringReader(sql));
    parser.setOriginalSql(sql);
    parser.setTabSize(1);
    parser.setQuotedCasing(config.quotedCasing());
    parser.setUnquotedCasing(config.unquotedCasing());
    parser.setIdentifierMaxLength(config.identifierMaxLength());
    parser.setConformance(config.conformance());
    switch (config.quoting()) {
      case DOUBLE_QUOTE:
        parser.switchTo("DQID");
        break;
      case BACK_TICK:
        parser.switchTo("BTID");
        break;
      case BRACKET:
        parser.switchTo("DEFAULT");
        break;
      default:
        throw new IllegalArgumentException("Unsupported Quoting type: " + config.quoting());
    }
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

  public static SduSqlStatement userDefinedSqlStatement(String sql) throws Exception {
    SqlNodeList sqlNodes = parse(sql);
    // 表
    List<SduTable> sduTables = definedTables(sqlNodes);
    // 函数
    List<SduFunction> sduFunctions = definedFunctions(sqlNodes);
    //
    return SduSqlStatement.of(sduTables, sduFunctions);
  }

}
