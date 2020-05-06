package com.sdu.calcite.plan.catalog.exceptions;

import com.sdu.calcite.plan.catalog.SduObjectIdentifier;
import com.sdu.calcite.plan.catalog.SduObjectPath;

public class SduTableNotExistException extends RuntimeException{

  private static final String MSG = "Table %s does not exist in Catalog %s.";

  public SduTableNotExistException(SduObjectIdentifier objectIdentifier) {
    this(objectIdentifier.getCatalogName(), objectIdentifier.toObjectPath());
  }

  public SduTableNotExistException(String catalogName, SduObjectPath tablePath) {
    super(String.format(MSG, tablePath.getFullName(), catalogName));
  }

}
