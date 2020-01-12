package com.sdu.calcite.sql.table;

import lombok.Data;
import org.apache.calcite.sql.type.SqlTypeName;

@Data
public class XTableColumn {

  private String name;
  private SqlTypeName type;
  private String comment;

  public XTableColumn(String name, SqlTypeName type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }


}
