package com.sdu.calcite.sql;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.SduTableConfigImpl;
import com.sdu.calcite.api.SduTableConfig;
import com.sdu.calcite.api.SduTableEnvironment;
import com.sdu.calcite.api.internal.SduTableEnvironmentImpl;
import com.sdu.calcite.plan.SduRelOptimizerFactoryTest;
import com.sdu.calcite.plan.catalog.SduCatalog;
import com.sdu.calcite.plan.catalog.SduCatalogDatabase;
import com.sdu.calcite.plan.catalog.SduCatalogDatabaseImpl;
import com.sdu.calcite.plan.catalog.SduCatalogImpl;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Before;

public class SduSqlBaseTest {

  protected SduTableEnvironment tableEnv;

  static String readSqlText(String name) throws IOException {
    InputStream stream = SduSqlSyntaxTest.class.getResourceAsStream(name);
    byte[] content = new byte[stream.available()];
    int bytes = stream.read(content);
    if (bytes < 0) {
      throw new RuntimeException("undefined sql statement.");
    }
    stream.close();
    return new String(content);
  }

  static RelNode validateAndRel(SqlNodeList sqlNodes, SduTableEnvironment tableEnv) {
    // 若有计算列, 则首先注册CREATE FUNCTION
    boolean hasComputedColumn = sqlNodes.getList()
        .stream()
        .filter(sqlNode -> sqlNode instanceof SqlCreateTable)
        .anyMatch(sqlNode -> {
          SqlCreateTable createTable = (SqlCreateTable) sqlNode;
          return createTable.getColumns().getList()
              .stream()
              .anyMatch(node -> node instanceof SqlBasicCall);
        });


    if (hasComputedColumn) {
      // 注册自定义函数
      sqlNodes.getList()
          .stream()
          .filter(sqlNode -> sqlNode instanceof SqlCreateFunction)
          .forEach(tableEnv::validate);
      // 注册数据库表
      sqlNodes.getList()
          .stream()
          .filter(sqlNode -> sqlNode instanceof SqlCreateTable)
          .forEach(tableEnv::validate);
    } else {
      sqlNodes.getList()
          .stream()
          .filter(sqlNode -> !(sqlNode instanceof SqlInsert))
          .forEach(tableEnv::validate);
    }



    //
    List<SqlNode> dmlNodes = sqlNodes.getList()
        .stream()
        .filter(sqlNode -> sqlNode.isA(SqlKind.DML) || sqlNode.isA(SqlKind.QUERY))
        .collect(Collectors.toList());

    if (dmlNodes.size() != 1) {
      throw new RuntimeException();
    }

    SqlNode dmlNode = dmlNodes.get(0);
    System.out.println("SQL: " + dmlNode.toString());
    switch (dmlNode.getKind()) {
      case INSERT:
        SqlInsert insert = (SqlInsert) dmlNode;
        return tableEnv.toRel(insert.getSource());

      default:
        return tableEnv.toRel(dmlNode);
    }
  }

  static RelNode optimizer(RelNode relNode, SduTableEnvironment tableEnv) {
    return tableEnv.optimize(relNode);
  }

  @Before
  public void setup() {
    SduTableConfig tableConfig = new SduTableConfigImpl(
        false,
        null,
        null,
        null,
        ImmutableList.of(ConventionTraitDef.INSTANCE),
        null,
        new SduRelOptimizerFactoryTest()
    );

    tableEnv = SduTableEnvironmentImpl.create(tableConfig);

    // catalog
    SduCatalog catalog = new SduCatalogImpl("sdu");
    // database
    SduCatalogDatabase database = new SduCatalogDatabaseImpl(new HashMap<>(), "");
    catalog.createDatabase("zhh", database, true);

    tableEnv.registerCatalog("sdu", catalog);
    tableEnv.useCatalog("sdu");
    tableEnv.useDatabase("zhh");

  }

}
