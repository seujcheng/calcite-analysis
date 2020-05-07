package com.sdu.calcite.plan.catalog.exceptions;

import com.sdu.calcite.plan.catalog.SduObjectPath;

public class SduFunctionAlreadyExistException extends RuntimeException {

  private static final String MSG = "Function %s already exists in Catalog %s.";

  public SduFunctionAlreadyExistException(String catalogName, SduObjectPath functionPath) {
    this(catalogName, functionPath, null);
  }

  public SduFunctionAlreadyExistException(String catalogName, SduObjectPath functionPath, Throwable cause) {
    super(String.format(MSG, functionPath.getFullName(), catalogName), cause);
  }

}
