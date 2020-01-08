package com.sdu.calcite.sql.table;

import lombok.Data;
import org.apache.calcite.rel.type.RelDataType;

@Data
public class XTableColumn {

  private String name;
  private RelDataType type;
  private String comment;

  public XTableColumn(String name, RelDataType type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }


}
