package com.sdu.calcite.sql.parser;

import static java.lang.String.format;

import com.sdu.calcite.SduCalciteConfig;
import com.sdu.calcite.SduCalciteRuleSetConfigBuilder;
import com.sdu.calcite.SduCalciteSqlSyntaxChecker;
import com.sdu.calcite.plan.SduCalciteSqlOptimizer;
import com.sdu.sql.entry.SduInsert;
import com.sdu.sql.entry.SduSqlStatement;
import com.sdu.sql.parse.SduCalciteSqlParser;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;

public class SduSqlParserTest {

  private SduCalciteSqlOptimizer optimizer;

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

  @Before
  public void setup() {
    SduCalciteConfig config = () -> SduCalciteRuleSetConfigBuilder.builder().build();
    optimizer = new SduCalciteSqlOptimizer(config);
  }

  @Test
  public void testSimpleFunction() throws Exception {
    String path = "/simple.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    Map<SduInsert, RelNode> res = SduCalciteSqlSyntaxChecker.sqlSyntaxOptimizer(statement, optimizer);
    for (Entry<SduInsert, RelNode> entry : res.entrySet()) {
      RelNode relNode = entry.getValue();
      System.out.println(RelOptUtil.toString(relNode));
    }
  }

  @Test
  public void testScalarFunction() throws Exception {
    String path = "/sql.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    Map<SduInsert, RelNode> res = SduCalciteSqlSyntaxChecker.sqlSyntaxOptimizer(statement, optimizer);
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
    Map<SduInsert, RelNode> res = SduCalciteSqlSyntaxChecker.sqlSyntaxOptimizer(statement, optimizer);
    for (Entry<SduInsert, RelNode> entry : res.entrySet()) {
      RelNode relNode = entry.getValue();
      System.out.println(RelOptUtil.toString(relNode));
    }
  }

}
