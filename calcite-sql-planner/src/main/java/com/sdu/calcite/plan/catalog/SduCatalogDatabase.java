package com.sdu.calcite.plan.catalog;

import java.util.Map;

public interface SduCatalogDatabase {

  Map<String, String> getProperties();

  String getComment();

  SduCatalogDatabase copy();

}
