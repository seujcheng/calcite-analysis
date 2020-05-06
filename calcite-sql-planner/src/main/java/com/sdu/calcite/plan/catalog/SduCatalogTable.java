package com.sdu.calcite.plan.catalog;

import com.sdu.sql.entry.SduTableColumn;
import java.util.List;
import java.util.Map;

public interface SduCatalogTable {

  Map<String, String> getProperties();

  List<SduTableColumn> getColumns();

  String getComment();

  SduCatalogTable copy();

}
