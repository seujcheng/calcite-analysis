package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.plan.SduCalciteTypeFactory;
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
    SduCalciteTypeFactory calciteTypeFactory = (SduCalciteTypeFactory) typeFactory;
    String[] fieldNames = new String[catalogTable.getColumns().size()];
    String[] fieldTypes = new String[catalogTable.getColumns().size()];
    for (int i = 0; i < catalogTable.getColumns().size(); ++i) {
      SduCatalogTableColumn column = catalogTable.getColumns().get(i);
      fieldNames[i] = column.getName();
      fieldTypes[i] = column.getType();
    }
    return calciteTypeFactory.buildRelDataType(fieldNames, fieldTypes);
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
