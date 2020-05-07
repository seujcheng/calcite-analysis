package com.sdu.calcite;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;

public class CalciteSchemaBuilder {

  private CalciteSchemaBuilder() {
  }

  public static CalciteSchema asRootSchema(Schema root) {
    return CalciteSchema.createRootSchema(
        false,
        false,
        "",
        root);
  }

}
