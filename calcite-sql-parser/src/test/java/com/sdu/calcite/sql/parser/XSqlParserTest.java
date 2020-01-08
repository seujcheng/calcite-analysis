package com.sdu.calcite.sql.parser;

import static java.lang.String.format;

import com.sdu.calcite.sql.planner.XSqlParser;
import java.io.IOException;
import java.io.InputStream;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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
    for (SqlNode sqlNode : sqlNodes) {
      System.out.println(sqlNode.toString());
    }
  }


}
