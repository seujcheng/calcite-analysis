package com.sdu.calcite.plan;

import com.sdu.calcite.plan.catalog.SduCalciteTable;
import com.sdu.calcite.prepare.SduCatalogSourceTable;
import com.sdu.calcite.prepare.SduPreparingTable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.PreparingTable;
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

  @Override
  public PreparingTable getTable(List<String> names) {
    Prepare.PreparingTable originRelOptTable = super.getTable(names);
    if (originRelOptTable == null) {
      return null;
    }
    SduCalciteTable calciteTable = originRelOptTable.unwrap(SduCalciteTable.class);
    if (calciteTable != null) {
      // PreparingTable用于构建RelNode, 这里重新此方法是用于计算列的处理.
      return new SduCatalogSourceTable(originRelOptTable.getRelOptSchema(),
          originRelOptTable.getRowType(),
          originRelOptTable.getQualifiedName(),
          calciteTable);
    }
    return originRelOptTable;
  }

}
