package com.sdu.calcite.schema;

import com.sdu.calcite.entry.SduOption;
import com.sdu.calcite.entry.SduTableColumn;
import com.sdu.calcite.types.SduTypeFactory;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.schema.impl.AbstractTable;

public class SduCalciteTable extends AbstractTable {

  private List<SduTableColumn> columns;
  @SuppressWarnings("unused")
  private Map<String, SduOption> properties;

  public SduCalciteTable(List<SduTableColumn> columns, Map<String, SduOption> properties) {
    this.columns = columns;
    this.properties = properties;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    SduTypeFactory sduTypeFactory = (SduTypeFactory) typeFactory;

    RelDataTypeFactory.Builder builder = new Builder(typeFactory);

    for (SduTableColumn column : columns) {
      builder.add(column.getName(), sduTypeFactory.createSqlType(column.getType()));
    }

    return builder.build();
  }

}
