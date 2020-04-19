package com.sdu.sql.entry;

import com.sdu.sql.parse.SduFunctionCatalog;
import java.util.List;
import lombok.Getter;

public class SduSqlStatement {

  @Getter
  private List<SduTable> tables;

  @Getter
  private SduFunctionCatalog functionCatalog;

  @Getter
  private List<SduInsert> inserts;

  private SduSqlStatement(List<SduTable> tables, List<SduFunction> functions, List<SduInsert> inserts) {
    this.tables = tables;
    this.inserts = inserts;
    functionCatalog = new SduFunctionCatalog();
    if (functions != null) {
      for (SduFunction function : functions) {
        functionCatalog.registerUserDefinedFunction(function.getName(), function);
      }
    }
  }

  public static SduSqlStatement of(List<SduTable> tables, List<SduFunction> functions, List<SduInsert> inserts) {
    return new SduSqlStatement(tables, functions, inserts);
  }

}
