package com.sdu.calcite.plan.catalog;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;

public class SduObjectIdentifier implements Serializable {

  private final String catalogName;

  private final String databaseName;

  private final String objectName;

  public static SduObjectIdentifier of(String catalogName, String databaseName, String objectName) {
    return new SduObjectIdentifier(catalogName, databaseName, objectName);
  }

  private SduObjectIdentifier(String catalogName, String databaseName, String objectName) {
    this.catalogName = Preconditions.checkNotNull(catalogName, "Catalog name must not be null.");
    this.databaseName = Preconditions.checkNotNull(databaseName, "Database name must not be null.");
    this.objectName = Preconditions.checkNotNull(objectName, "Object name must not be null.");
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getObjectName() {
    return objectName;
  }

  public SduObjectPath toObjectPath() {
    return SduObjectPath.of(databaseName, objectName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SduObjectIdentifier that = (SduObjectIdentifier) o;
    return catalogName.equals(that.catalogName) &&
        databaseName.equals(that.databaseName) &&
        objectName.equals(that.objectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogName, databaseName, objectName);
  }
}
