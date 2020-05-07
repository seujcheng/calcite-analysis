package com.sdu.calcite.plan.catalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SduCatalogTable {

  List<SduCatalogTableColumn> getColumns();

  Optional<Map<String, String>> getProperties();

  Optional<String> getComment();

  SduCatalogTable copy();

}
