package com.sdu.calcite.sql;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.SduTableConfigImpl;
import com.sdu.calcite.api.SduTableConfig;
import com.sdu.calcite.api.SduTableEnvironment;
import com.sdu.calcite.api.internal.SduTableEnvironmentImpl;
import com.sdu.calcite.plan.SduRelOptimizerFactoryTest;
import com.sdu.calcite.plan.catalog.SduCatalog;
import com.sdu.calcite.plan.catalog.SduCatalogDatabase;
import com.sdu.calcite.plan.catalog.SduCatalogDatabaseImpl;
import com.sdu.calcite.plan.catalog.SduCatalogImpl;
import java.util.HashMap;
import org.apache.calcite.plan.ConventionTraitDef;
import org.junit.Before;

public class SduSqlBaseTest {

  protected SduTableEnvironment tableEnv;

  @Before
  public void setup() {
    SduTableConfig tableConfig = new SduTableConfigImpl(
        false,
        null,
        null,
        null,
        ImmutableList.of(ConventionTraitDef.INSTANCE),
        null,
        new SduRelOptimizerFactoryTest()
    );

    tableEnv = SduTableEnvironmentImpl.create(tableConfig);

    // catalog
    SduCatalog catalog = new SduCatalogImpl("sdu");
    // database
    SduCatalogDatabase database = new SduCatalogDatabaseImpl(new HashMap<>(), "");
    catalog.createDatabase("zhh", database, true);

    tableEnv.registerCatalog("sdu", catalog);
    tableEnv.useCatalog("sdu");
    tableEnv.useDatabase("zhh");

  }

}
