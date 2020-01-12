package com.sdu.calcite.sql.table;

import java.util.Objects;
import lombok.Data;

@Data
public class XNodePath {

  private String tableName;
  private String columnName;


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    XNodePath xNodePath = (XNodePath) o;
    return tableName.equals(xNodePath.tableName) &&
        columnName.equals(xNodePath.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnName);
  }
}
