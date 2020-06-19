package com.sdu.calcite.sql;

import com.sdu.calcite.api.SduTableEnvironment;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;

public class SduSqlSyntaxTest extends SduSqlBaseTest {

  private static String readSqlText(String name) throws IOException {
    InputStream stream = SduSqlSyntaxTest.class.getResourceAsStream(name);
    byte[] content = new byte[stream.available()];
    int bytes = stream.read(content);
    if (bytes < 0) {
      throw new RuntimeException("undefined sql statement.");
    }
    stream.close();
    return new String(content);
  }



  @Test
  public void testInsertSelectSql() throws Exception {
    String path = "/sql1.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    RelNode optimized = optimizer(relNode, tableEnv);
    System.out.println("After optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(optimized));
  }

  @Test
  public void testInsertSql() throws Exception {
    String path = "/sql2.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    RelNode optimized = optimizer(relNode, tableEnv);
    System.out.println("After optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(optimized));
  }


  @Test
  public void testComputedColumn() throws Exception {
    String path = "/sql3.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    RelNode optimized = optimizer(relNode, tableEnv);
    System.out.println("After optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(optimized));
  }

  @Test
  public void testDeleteSql() throws Exception {
    String path = "/sql4.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    System.out.println("Before optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(relNode));
  }

  @Test
  public void testUpdateSql() throws Exception {
    String path = "/sql5.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    System.out.println("Before optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(relNode));
  }

  @Test
  public void testTopN() throws Exception{
    String path = "/sql6.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    RelNode optimized = optimizer(relNode, tableEnv);
    System.out.println("After optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(optimized));
  }

  @Test
  public void testSelectSql() throws Exception {
    String path = "/sql7.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    System.out.println("Before optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(relNode));
  }

  @Test
  public void testUnionSql() throws Exception {
    String path = "/sql8.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    System.out.println("Before optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(relNode));
  }

  @Test
  public void testInternalFunctionSql() throws Exception {
    String path = "/sql9.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    RelDataType resultType = relNode.getRowType();
    for (RelDataTypeField field : resultType.getFieldList()) {
      System.out.println(field.getType());
    }
    System.out.println("Before optimize:");
    System.out.println();
    System.out.println(RelOptUtil.toString(relNode));
  }

  private static RelNode validateAndRel(SqlNodeList sqlNodes, SduTableEnvironment tableEnv) {
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

  private static RelNode optimizer(RelNode relNode, SduTableEnvironment tableEnv) {
    return tableEnv.optimize(relNode);
  }

}
