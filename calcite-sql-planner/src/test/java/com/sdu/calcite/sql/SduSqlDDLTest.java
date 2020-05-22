package com.sdu.calcite.sql;

import com.sdu.calcite.sql.ddl.SqlCreateTable;
import com.sdu.calcite.sql.ddl.SqlWatermark;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

public class SduSqlDDLTest extends SduSqlBaseTest {

  @Test
  public void testCreateTableWithComputedColumn() {
    String sql = "CREATE TABLE t (\n"
               + "  id INT, \n"
               + "  uname VARCHAR, \n"
               + "  address VARCHAR, \n"
               + "  country AS GET_COUNTRY(address) \n"
               + ")";

    SqlNode sqlNode = tableEnv.parseStmt(sql);
    Assert.assertSame(SqlKind.CREATE_TABLE, sqlNode.getKind());

    SqlCreateTable createTable = (SqlCreateTable) sqlNode;

    SqlNode computedColumn = createTable.getColumns()
        .getList()
        .stream()
        .filter(column -> column instanceof SqlBasicCall)
        .findAny().orElse(null);
    Assert.assertNotNull(computedColumn);

    System.out.println(computedColumn.toString());
  }

  @Test
  public void testCreateTableWithWatermark() {
    String sql = "CREATE TABLE t (\n"
               + "  uid INT, \n"
               + "  poiId INT, \n"
               + "  click TIMESTAMP, \n"
               + "  WATERMARK FOR click AS click - 1000 \n"
               + ")";

    SqlNode sqlNode = tableEnv.parseStmt(sql);
    Assert.assertSame(SqlKind.CREATE_TABLE, sqlNode.getKind());

    SqlCreateTable createTable = (SqlCreateTable) sqlNode;

    SqlWatermark watermark = createTable.getWatermark().orElse(null);
    Assert.assertNotNull(watermark);

    System.out.println(watermark.toString());
  }

}
