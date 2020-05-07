package com.sdu.calcite.plan.catalog.exceptions;

public class SduDatabaseNotExistException extends RuntimeException{

  private static final String MSG = "Database %s does not exist in Catalog %s.";

  public SduDatabaseNotExistException(String catalogName, String databaseName, Throwable cause) {
    super(String.format(MSG, databaseName, catalogName), cause);
  }


  public SduDatabaseNotExistException(String catalogName, String databaseName) {
    this(catalogName, databaseName, null);
  }

}
