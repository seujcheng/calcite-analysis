package com.sdu.calcite.plan;

import com.sun.org.apache.xml.internal.resolver.CatalogManager;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlNode;

public class SqlNodeConverter {

  private final CatalogManager catalogManager;
  private final SduCalciteSqlPlanner plannerSupplier;

  private SqlNodeConverter(CatalogManager catalogManager, SduCalciteSqlPlanner planner) {
    this.catalogManager = catalogManager;
    this.plannerSupplier = planner;
  }


  public static void convert(
      Supplier<SduCalciteSqlPlanner> plannerSupplier,
      CatalogManager catalogManager,
      SqlNode sqlNode) {
    SduCalciteSqlPlanner planner = plannerSupplier.get();
    SqlNode validated = planner.validate(sqlNode);

    SqlNodeConverter converter = new SqlNodeConverter(catalogManager, planner);

    String className = sqlNode.getClass().getSimpleName();

    switch (className) {
      // TODO: SqlCreateDatabase

      case "SqlCreateTable":

        break;

      case "SqlCreateFunction":
        break;

    }

  }

}
