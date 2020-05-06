package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.api.SduCatalogManager;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

public class SduCatalogSchema extends SduSchema {

  private final String catalogName;
  private final SduCatalogManager catalogManager;

  SduCatalogSchema(String catalogName, SduCatalogManager catalogManager) {
    this.catalogName = catalogName;
    this.catalogManager = catalogManager;
  }

  @Override
  public Table getTable(String name) {
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return new HashSet<>();
  }

  @Override
  public Schema getSubSchema(String name) {
    if (catalogManager.schemaExists(catalogName, name)) {
      return new SduCatalogDatabaseSchema(catalogName, name, catalogManager);
    }
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return catalogManager.listSchemas(catalogName);
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
