package com.sdu.calcite.sql;

import com.sdu.calcite.plan.exec.DataTransformation;
import com.sdu.calcite.plan.nodes.SduExecuteRel;
import com.sdu.calcite.table.data.SduGenericRowData;
import com.sdu.calcite.table.data.SduRowData;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Assert;
import org.junit.Test;

public class SduSqlTranslateTest extends SduSqlBaseTest {

  @Test
  public void testSqlTranslate() throws Exception {
    String path = "/sql10.txt";
    String sqlText = readSqlText(path);
    SqlNodeList sqlNodes = tableEnv.parseStmtList(sqlText);
    RelNode relNode = validateAndRel(sqlNodes, tableEnv);
    RelNode optimized = optimizer(relNode, tableEnv);
    Assert.assertTrue(optimized instanceof SduExecuteRel);
    SduExecuteRel executeRel = (SduExecuteRel) optimized;

    DataTransformation transformation = executeRel.translateToPlanInternal();
    List<SduRowData> result = transformation.getOutput();
    for (SduRowData ret : result) {
      Assert.assertTrue(ret instanceof SduGenericRowData);
      SduGenericRowData data = (SduGenericRowData) ret;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < ret.getArity(); ++i) {
        if (i == 0) {
          sb.append(data.getField(i));
        } else {
          sb.append(", ");
          sb.append(data.getField(i));
        }
      }
      System.out.println(sb.toString());
    }
  }

}
