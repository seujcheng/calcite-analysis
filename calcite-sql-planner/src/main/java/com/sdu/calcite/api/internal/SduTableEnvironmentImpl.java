package com.sdu.calcite.api.internal;

import static com.sdu.calcite.api.SduCatalogManager.DEFAULT_CATALOG_NAME;

import com.sdu.calcite.SduParser;
import com.sdu.calcite.api.SduCatalogManager;
import com.sdu.calcite.api.SduTableConfig;
import com.sdu.calcite.api.SduTableEnvironment;
import com.sdu.calcite.plan.SduPlanner;
import com.sdu.calcite.plan.SduPlannerImpl;
import com.sdu.calcite.plan.catalog.SduCatalog;
import com.sdu.calcite.plan.catalog.SduCatalogImpl;
import com.sdu.calcite.plan.catalog.SduFunctionCatalog;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public class SduTableEnvironmentImpl implements SduTableEnvironment {

  private final SduTableConfig tableConfig;
  private final SduFunctionCatalog functionCatalog;
  private final SduCatalogManager catalogManager;
  private final SduPlanner planner;
  private final SduParser parser;

  private SduTableEnvironmentImpl(
      SduTableConfig tableConfig,
      SduFunctionCatalog functionCatalog,
      SduCatalogManager catalogManager,
      SduPlanner planner) {
    this.tableConfig = tableConfig;
    this.functionCatalog = functionCatalog;
    this.catalogManager = catalogManager;
    this.planner = planner;
    this.parser = planner.getParser();
  }

  @Override
  public void registerCatalog(String catalogName, SduCatalog catalog) {
    catalogManager.registerCatalog(catalogName, catalog);
  }

  @Override
  public void useCatalog(String catalogName) {
    catalogManager.setCurrentCatalog(catalogName);
  }

  @Override
  public String getCurrentCatalog() {
    return catalogManager.getCurrentCatalog();
  }

  @Override
  public String getCurrentDatabase() {
    return catalogManager.getCurrentDatabaseName();
  }

  @Override
  public void useDatabase(String databaseName) {
    catalogManager.setCurrentDatabase(databaseName);
  }

  @Override
  public SqlNodeList parseStmtList(String stmt) {
    return parser.parseStmtList(stmt);
  }

  @Override
  public SqlNode parseStmt(String stmt) {
    return parser.parseStmt(stmt);
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) {
    return planner.validate(sqlNode);
  }

  @Override
  public RelNode toRel(SqlNode sqlNode) {
    return planner.toRel(sqlNode);
  }

  @Override
  public RelNode optimize(RelNode relNode) {
    return planner.optimize(relNode);
  }

  public static SduTableEnvironment create(SduTableConfig tableConfig) {
    SduCatalogManager catalogManager = new SduCatalogManager(DEFAULT_CATALOG_NAME,
        new SduCatalogImpl(DEFAULT_CATALOG_NAME));

    SduFunctionCatalog functionCatalog = new SduFunctionCatalog();

    SduPlanner planner = new SduPlannerImpl(tableConfig, functionCatalog, catalogManager);

    return new SduTableEnvironmentImpl(tableConfig, functionCatalog, catalogManager, planner);
  }

}
