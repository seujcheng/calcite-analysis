package com.sdu.calcite.plan;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

class SduCalciteCatalogReader extends CalciteCatalogReader {

  SduCalciteCatalogReader(CalciteSchema rootSchema,
      List<List<String>> defaultSchema,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema,
        SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
        Stream.concat(defaultSchema.stream(), Stream.of(Collections.<String>emptyList()))
            .collect(Collectors.toList()),
        typeFactory,
        config);
  }

}
