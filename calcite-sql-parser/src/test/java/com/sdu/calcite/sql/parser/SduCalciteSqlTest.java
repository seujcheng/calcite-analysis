package com.sdu.calcite.sql.parser;

import com.sdu.calcite.SduCalciteConfig;
import com.sdu.calcite.SduCalciteConfigImpl;
import com.sdu.calcite.SduPlanner;
import com.sdu.calcite.plan.SduCalciteOptimizer;
import com.sdu.calcite.plan.SduCalcitePlanningConfigBuilder;
import com.sdu.calcite.sql.plan.SduCalciteOptimizerTest;
import com.sdu.sql.entry.SduInsert;
import com.sdu.sql.entry.SduSqlStatement;
import com.sdu.sql.parse.SduCalciteSqlParser;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;

public class SduCalciteSqlTest {

  private Function<SduSqlStatement, SduCalcitePlanningConfigBuilder> calcitePlannerBuilderSupplier;
  private Function<SduCalcitePlanningConfigBuilder, SduCalciteOptimizer> calciteOptimizerSupplier;

  private static String readSqlText(String name) throws IOException {
    InputStream stream = SduCalciteSqlTest.class.getResourceAsStream(name);
    byte[] content = new byte[stream.available()];
    int bytes = stream.read(content);
    if (bytes < 0) {
      throw new RuntimeException("undefined sql statement.");
    }
    stream.close();
    return new String(content);
  }

  private SduPlanner createSduPlanner(SduSqlStatement statement) {
    return new SduPlanner(calcitePlannerBuilderSupplier.apply(statement), calciteOptimizerSupplier);
  }

  @Before
  public void setup() {
    calcitePlannerBuilderSupplier = statement -> {
      SduCalciteConfig calciteConfig = SduCalciteConfigImpl.fromSduSqlStatement(statement);
      return new SduCalcitePlanningConfigBuilder(calciteConfig);
    };

    calciteOptimizerSupplier = SduCalciteOptimizerTest::new;
  }

  @Test
  public void testSimpleFunction() throws Exception {
    String path = "/simple.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    SduPlanner planner = createSduPlanner(statement);

    for (SduInsert insert : statement.getInserts()) {
      RelNode optimized = planner.optimizePlan(insert.getSqlNode());
      System.out.println(RelOptUtil.toString(optimized));
    }
  }

  @Test
  public void testScalarFunction() throws Exception {
    String path = "/sql.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    SduPlanner planner = createSduPlanner(statement);

    for (SduInsert insert : statement.getInserts()) {
      RelNode optimized = planner.optimizePlan(insert.getSqlNode());
      System.out.println(RelOptUtil.toString(optimized));
    }
  }

  @Test
  public void testTopN() throws Exception{
    String path = "/topN.txt";
    String sqlText = readSqlText(path);
    SduSqlStatement statement = SduCalciteSqlParser.userDefinedSqlStatement(sqlText);
    SduPlanner planner = createSduPlanner(statement);

    for (SduInsert insert : statement.getInserts()) {
      RelNode optimized = planner.optimizePlan(insert.getSqlNode());
      System.out.println(RelOptUtil.toString(optimized));
    }
  }

}
