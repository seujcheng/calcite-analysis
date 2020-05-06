package com.sdu.calcite.plan.catalog.exceptions;

import static java.lang.String.format;

public class SduCatalogNotExistException extends RuntimeException{

  public SduCatalogNotExistException(String catalogName) {
    super(format("A catalog with name [%s] does not exist.", catalogName));
  }



}
