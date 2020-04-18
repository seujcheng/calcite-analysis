package com.sdu.calcite.sql.parser;

import static java.lang.String.format;

import com.sdu.calcite.entry.SduInsert;
import com.sdu.calcite.entry.SduSqlStatement;
import com.sdu.calcite.util.SduCalciteSqlParser;
import com.sdu.calcite.util.SduCalciteSqlSyntaxChecker;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.junit.Test;

public class SduSqlParserTest {

  private static String readSqlText(String name) throws IOException {
    InputStream stream = SduSqlParserTest.class.getResourceAsStream(name);
    byte[] content = new byte[stream.available()];
    int bytes = stream.read(content);
    if (bytes < 0) {
      System.out.println(format(">>>>>>>>>>>文件[%s]内容空<<<<<<<<<<<<<", name));
    }
    stream.close();
    return new String(content);
  }

  @Test
  public void testScalarFunction() throws Exception {
    String path = "/sql.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    Map<SduInsert, RelNode> res = SduCalciteSqlSyntaxChecker.sqlSyntaxValidate(statement);
    for (Entry<SduInsert, RelNode> entry : res.entrySet()) {
      RelNode relNode = entry.getValue();
      System.out.println(RelOptUtil.toString(relNode));
    }
  }

  @Test
  public void testTopN() throws Exception{
    String path = "/topN.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    Map<SduInsert, RelNode> res = SduCalciteSqlSyntaxChecker.sqlSyntaxValidate(statement);
    for (Entry<SduInsert, RelNode> entry : res.entrySet()) {
      RelNode relNode = entry.getValue();
      System.out.println(RelOptUtil.toString(relNode));
    }
  }

}
