package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.api.SduCatalogManager;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

/*
 * Calcite Schema Tree:
 *
 *                                                SduCatalogManagerSchema
 *                                                          |
 *                                                          |
 *                              +---------------------------+---------------------------+
 *                              |                                                       |
 *                             \|/                                                     \|/
 *                          SduCatalogSchema              ......                 SduCatalogSchema
 *                              |                                                       |
 *                              |                                                       |
 *                +-------------+-------------+                           +-------------+-------------+
 *                |                           |                           |                           |
 *               \|/                         \|/                         \|/                         \|/
 *    SduCatalogDatabaseSchema  ...  SduCatalogDatabaseSchema  SduCatalogDatabaseSchema  ...  SduCatalogDatabaseSchema
 * */
public class SduCatalogManagerSchema extends SduSchema {

  private final SduCatalogManager catalogManager;

  public SduCatalogManagerSchema(SduCatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  @Override
  public Table getTable(String name) {
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return null;
  }

  @Override
  public Schema getSubSchema(String name) {
    if (catalogManager.schemaExists(name)) {
      return new SduCatalogSchema(name, catalogManager);
    }

    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return catalogManager.listCatalogs();
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
