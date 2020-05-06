package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.api.SduCatalogManager;
import com.sdu.calcite.plan.catalog.exceptions.SduTableNotExistException;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

public class SduCalciteDatabase extends SduSchema {

  // 数据库名
  private final String databaseName;
  private final String catalogName;
  private final SduCatalogManager catalogManager;

  SduCalciteDatabase(String databaseName, String catalogName, SduCatalogManager catalogManager) {
    this.databaseName = databaseName;
    this.catalogName = catalogName;
    this.catalogManager = catalogManager;
  }

  @Override
  public Table getTable(String name) {
    final SduObjectIdentifier objectIdentifier = SduObjectIdentifier.of(catalogName, databaseName, name);

    return catalogManager.getTable(objectIdentifier)
        .map(catalogTable -> new SduCalciteTable(objectIdentifier, catalogTable))
        .orElseThrow(() -> new SduTableNotExistException(objectIdentifier));
  }

  @Override
  public Set<String> getTableNames() {
    return null;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return null;
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

}
