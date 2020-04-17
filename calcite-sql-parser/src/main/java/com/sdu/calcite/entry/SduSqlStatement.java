package com.sdu.calcite.entry;

import java.util.List;
import lombok.Data;

@Data
public class SduSqlStatement {

  private List<SduTable> tables;

  private List<SduFunction> functions;

  private SduSqlStatement(List<SduTable> tables, List<SduFunction> functions) {
    this.tables = tables;
    this.functions = functions;
  }

  public static SduSqlStatement of(List<SduTable> tables, List<SduFunction> functions) {
    return new SduSqlStatement(tables, functions);
  }
}
