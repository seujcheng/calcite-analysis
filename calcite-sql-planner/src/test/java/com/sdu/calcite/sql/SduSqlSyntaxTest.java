package com.sdu.calcite.sql;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;

public class SduSqlSyntaxTest extends SduSqlBaseTest {

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

}
