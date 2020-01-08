package com.sdu.calcite.sql.table;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.schema.impl.AbstractTable;

public class XTable extends AbstractTable {

  private final List<XTableColumn> columns;

  public XTable(List<XTableColumn> columns) {
    this.columns = columns;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = new Builder(typeFactory);

    for (XTableColumn column : columns) {
      builder.add(column.getName(), column.getType());
    }

    return builder.build();
  }

}
