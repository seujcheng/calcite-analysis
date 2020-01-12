package com.sdu.calcite.sql.parser;

import static java.lang.String.format;

import com.sdu.calcite.sql.XCalciteUtils;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import com.sdu.calcite.sql.ddl.SqlUseFunction;
import com.sdu.calcite.sql.planner.XSqlParser;
import com.sdu.calcite.sql.planner.XSqlPlanner;
import com.sdu.calcite.sql.table.XNodePath;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Before;
import org.junit.Test;

public class XSqlParserTest {

  private String readSqlText(String name) throws IOException {
    InputStream stream = this.getClass().getResourceAsStream(name);
    byte[] content = new byte[stream.available()];
    int bytes = stream.read(content);
    if (bytes < 0) {
      System.out.println(format(">>>>>>>>>>>文件[%s]内容空<<<<<<<<<<<<<", name));
    }
    stream.close();

    return new String(content);
  }

  @Test
  public void testSql() throws Exception {
    String path = "/sql.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = XSqlParser.parse(sqlText);
    for (SqlNode sqlNode : sqlNodes) {
      System.out.println(sqlNode.toString());
    }
  }

  @Test
  public void testSqlAggregateOnJoin() throws Exception {
    String path = "/sql_join.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = XSqlParser.parse(sqlText);
    List<SqlCreateTable> createTables = new ArrayList<>();
    List<SqlUseFunction> functions = new ArrayList<>();
    SqlNode select = null;
    for (SqlNode sqlNode : sqlNodes) {
      if (sqlNode instanceof SqlCreateTable) {
        createTables.add((SqlCreateTable) sqlNode);
      } else if (sqlNode instanceof SqlUseFunction) {
        functions.add((SqlUseFunction) sqlNode);
      } else if (sqlNode instanceof SqlInsert) {
        if (select != null) {
          throw new RuntimeException("Only support one 'INSERT' statement ..");
        }
        SqlInsert sqlInsert = (SqlInsert) sqlNode;
        select = sqlInsert.getSource();
      }
    }

    XSqlPlanner planner = XCalciteUtils.createXSqlPlanner(createTables, functions);
    RelRoot relRoot = planner.validateAndRel(select);

    Set<XNodePath> groupNodes = planner.getGroupNodeMeta((SqlSelect) select);

    System.out.println(groupNodes);
    System.out.println(RelOptUtil.toString(relRoot.rel));
  }


}
