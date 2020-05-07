package com.sdu.calcite.plan.catalog;

import java.util.Optional;

public interface SduCatalogTableColumn {

  String getName();

  String getType();

  Optional<String> getExpr();

  Optional<String> getComment();

}
