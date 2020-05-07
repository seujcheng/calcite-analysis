package com.sdu.calcite.plan.catalog;

import java.util.Map;

public interface SduCatalogFunction {

  Map<String, String> getProperties();

  boolean isTemporary();

  SduCatalogFunction copy();

}
