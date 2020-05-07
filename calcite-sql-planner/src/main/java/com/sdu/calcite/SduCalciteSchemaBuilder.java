package com.sdu.calcite;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;

public class SduCalciteSchemaBuilder {

  private SduCalciteSchemaBuilder() {

  }

  public static CalciteSchema asRootSchema(Schema root) {
    return CalciteSchema.createRootSchema(false, false, "", root);
  }

}
