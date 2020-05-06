package com.sdu.calcite.plan.catalog;

import javax.annotation.Nonnull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.schema.impl.AbstractTable;

public class SduCalciteTable extends AbstractTable implements TemporalTable {

  private final SduObjectIdentifier tableIdentifier;
  private final SduCatalogTable catalogTable;

  SduCalciteTable(SduObjectIdentifier tableIdentifier, SduCatalogTable catalogTable) {
    this.tableIdentifier = tableIdentifier;
    this.catalogTable = catalogTable;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return null;
  }

  @Nonnull
  @Override
  public String getSysStartFieldName() {
    return "sys_start";
  }

  @Nonnull
  @Override
  public String getSysEndFieldName() {
    return "sys_end";
  }

}
