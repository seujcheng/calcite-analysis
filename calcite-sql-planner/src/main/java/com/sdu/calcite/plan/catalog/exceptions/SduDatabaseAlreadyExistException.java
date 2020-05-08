package com.sdu.calcite.plan.catalog.exceptions;

public class SduDatabaseAlreadyExistException extends RuntimeException {

  private static final String MSG = "Database %s already exists in Catalog %s.";

  public SduDatabaseAlreadyExistException(String catalog, String database, Throwable cause) {
    super(String.format(MSG, database, catalog), cause);
  }

  public SduDatabaseAlreadyExistException(String catalog, String database) {
    this(catalog, database, null);
  }

}
