package com.sdu.calcite.plan.catalog;

import java.io.Serializable;
import java.util.Objects;

public class SduObjectPath implements Serializable {

  private final String databaseName;
  private final String objectName;

  public static SduObjectPath of(String databaseName, String objectName) {
    return new SduObjectPath(databaseName, objectName);
  }

  private SduObjectPath(String databaseName, String objectName) {
    this.databaseName = databaseName;
    this.objectName = objectName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getFullName() {
    return String.format("%s.%s", databaseName, objectName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SduObjectPath that = (SduObjectPath) o;
    return databaseName.equals(that.databaseName) &&
        objectName.equals(that.objectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, objectName);
  }
}
