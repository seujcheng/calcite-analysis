package com.sdu.calcite.entry;

import java.util.List;
import lombok.Data;

@Data
public class SduSqlStatement {

  private List<SduTable> tables;

  private List<SduFunction> functions;

  private List<SduInsert> inserts;

  private SduSqlStatement(List<SduTable> tables, List<SduFunction> functions, List<SduInsert> inserts) {
    this.tables = tables;
    this.functions = functions;
    this.inserts = inserts;
  }

  public static SduSqlStatement of(List<SduTable> tables, List<SduFunction> functions, List<SduInsert> inserts) {
    return new SduSqlStatement(tables, functions, inserts);
  }

}
