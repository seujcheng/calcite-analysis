package com.sdu.calcite.plan;

import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatcher;

public class SduCalciteCatalogReader extends CalciteCatalogReader {

  public SduCalciteCatalogReader(
      CalciteSchema rootSchema,
      SqlNameMatcher nameMatcher,
      List<List<String>> schemaPaths,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema, nameMatcher, schemaPaths, typeFactory, config);
  }

}
