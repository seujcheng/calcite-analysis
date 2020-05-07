package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.api.SduCatalogManager;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

public class SduCatalogDatabaseSchema extends SduSchema {

  private final String catalogName;
  // 数据库名
  private final String databaseName;
  private final SduCatalogManager catalogManager;

  SduCatalogDatabaseSchema(String catalogName, String databaseName, SduCatalogManager catalogManager) {
    this.catalogName = catalogName;
    this.databaseName = databaseName;
    this.catalogManager = catalogManager;
  }

  @Override
  public Table getTable(String name) {
    final SduObjectIdentifier objectIdentifier = SduObjectIdentifier.of(catalogName, databaseName, name);

    return catalogManager.getTable(objectIdentifier)
        .map(catalogTable -> new SduCalciteTable(objectIdentifier, catalogTable))
        .orElse(null);
  }

  @Override
  public Set<String> getTableNames() {
    return catalogManager.listTables(catalogName, databaseName);
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
    return Schemas.subSchemaExpression(parentSchema, name, getClass());
  }

  @Override
  public boolean isMutable() {
    return true;
  }

}
