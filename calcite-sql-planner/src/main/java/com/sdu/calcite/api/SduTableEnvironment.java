package com.sdu.calcite.api;

import com.sdu.calcite.plan.catalog.SduCatalog;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public interface SduTableEnvironment {

  // -------- catalog ----------

  void registerCatalog(String catalogName, SduCatalog catalog);

  void useCatalog(String catalogName);

  String getCurrentCatalog();

  void useDatabase(String databaseName);

  String getCurrentDatabase();

  // -------- sql --------
  SqlNodeList parseStmtList(String stmt);

  SqlNode parseStmt(String stmt);

  SqlNode validate(SqlNode sqlNode);

  RelNode toRel(SqlNode sqlNode);

  RelNode optimize(RelNode relNode);

}
