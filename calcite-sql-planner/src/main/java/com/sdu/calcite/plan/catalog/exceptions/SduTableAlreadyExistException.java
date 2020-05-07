package com.sdu.calcite.plan.catalog.exceptions;

    import com.sdu.calcite.plan.catalog.SduObjectPath;

public class SduTableAlreadyExistException extends RuntimeException {

  private static final String MSG = "Table %s already exists in Catalog %s.";

  public SduTableAlreadyExistException(String catalogName, SduObjectPath tablePath) {
    super(String.format(MSG, catalogName, tablePath.getFullName()));
  }

}
