package com.sdu.calcite.metadata;

import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class SduCalciteRelMdRowCount extends RelMdRowCount {

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
      BuiltInMethod.ROW_COUNT.method, new SduCalciteRelMdRowCount());

  private SduCalciteRelMdRowCount() {

  }

  @Override
  public Double getRowCount(Calc rel, RelMetadataQuery mq){
    return rel.estimateRowCount(mq);
  }

}
