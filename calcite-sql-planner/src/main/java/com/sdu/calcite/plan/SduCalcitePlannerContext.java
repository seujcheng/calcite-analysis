package com.sdu.calcite.plan;

import static java.lang.String.valueOf;
import static org.apache.calcite.config.CalciteConnectionProperty.CASE_SENSITIVE;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.SduCalciteConfig;
import com.sdu.calcite.catelog.SduCalciteFunctionOperatorTable;
import com.sdu.calcite.catelog.SduCalciteInternalOperatorTable;
import com.sdu.calcite.plan.cost.SduRelOptCostFactory;
import com.sdu.sql.parse.SduFunctionCatalog;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class SduCalcitePlannerContext {

  private SduCalciteConfig calciteConfig;

  private final Context context;
  private final RelOptPlanner planner;
  private final RelOptCostFactory costFactory;
  private final SduCalciteTypeFactory typeFactory;
  private final FrameworkConfig frameworkConfig;


  public SduCalcitePlannerContext(SduCalciteConfig calciteConfig) {
    this.calciteConfig = calciteConfig;
    this.context = calciteConfig.getContext().orElse(Contexts.empty());
    this.costFactory = calciteConfig.getRelOptCostFactory().orElse(new SduRelOptCostFactory());
    // TODO: 类型统一
    this.typeFactory = calciteConfig.getRelDataTypeFactory()
        .map(dataTypeFactory -> {
          if (dataTypeFactory instanceof SduCalciteTypeFactory) {
            return (SduCalciteTypeFactory) dataTypeFactory;
          }
          throw new RuntimeException("RelDataTypeFactory should be SduCalciteTypeFactory's instance");
        })
        .orElse(new SduCalciteTypeFactory());

    this.planner = new VolcanoPlanner(costFactory, context);
    this.frameworkConfig = createFrameworkConfig();
  }

  private FrameworkConfig createFrameworkConfig() {
    // register table schema
    CalciteSchema calciteSchema = calciteConfig.getCalciteSchema()
        .orElseThrow(() -> new RuntimeException("calcite schema not exist."));

    // register function schema
    SduFunctionCatalog functionCatalog = calciteConfig.getFunctionCatalog()
        .orElse(new SduFunctionCatalog());
    SqlOperatorTable functionOperatorTable = ChainedSqlOperatorTable
        .of(new SduCalciteInternalOperatorTable(),
            new SduCalciteFunctionOperatorTable(typeFactory, functionCatalog));

    // sql parser config
    SqlParser.Config parserConf = calciteConfig.getSqlParserConfig()
        .orElse(
            SqlParser.configBuilder()
                // 禁止转为大写
                .setUnquotedCasing(Casing.UNCHANGED)
                .setParserFactory(new SduCalciteSqlParserFactory())
                .build()
        );

    // SqlNode Convert RelNode Config
    SqlToRelConverter.Config sqlToRelConvertConf = calciteConfig.getSqlToRelConvertConfig()
        .orElse(
            SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                // TableScan --> LogicalTableScan(即: TableScan类型是LogicalTableScan)
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .withRelBuilderFactory(new SduCalciteRelBuilderFactory(context))
                .build()
        );

    // 构建RelNode默认特征
    List<RelTraitDef> traitDefs = calciteConfig.getDefaultRelTrait()
        .orElse(ImmutableList.of(ConventionTraitDef.INSTANCE));

    return Frameworks.newConfigBuilder()
        .defaultSchema(calciteSchema.plus())
        .parserConfig(parserConf)
        .costFactory(costFactory)
        .typeSystem(typeFactory.getTypeSystem())
        .operatorTable(functionOperatorTable)
        .sqlToRelConverterConfig(sqlToRelConvertConf)
        .traitDefs(traitDefs)
        .build();
  }

  private CalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity) {
    CalciteSchema rootSchema = CalciteSchema.from(frameworkConfig.getDefaultSchema());
    SqlParser.Config parserConfig = frameworkConfig.getParserConfig();
    boolean caseSensitive;
    if (lenientCaseSensitivity) {
      caseSensitive = false;
    } else {
      caseSensitive = parserConfig.caseSensitive();
    }
    Properties props = new Properties();
    props.setProperty(CASE_SENSITIVE.camelName(), valueOf(caseSensitive));
    // TODO: Catalog
    return new CalciteCatalogReader(
        rootSchema, Collections.emptyList(), typeFactory, new CalciteConnectionConfigImpl(props));
  }

  public SduCalciteSqlPlanner createCalcitePlanner() {
    Function<Boolean, CalciteCatalogReader> catalogReaderSupplier = this::createCatalogReader;
    return new SduCalciteSqlPlanner(frameworkConfig, catalogReaderSupplier, planner, typeFactory);
  }

  public SduCalciteRelBuilder createCalciteRelBuilder() {
    RelOptCluster cluster = SduCalciteRelOptClusterFactory.create(planner, new RexBuilder(typeFactory));
    CalciteCatalogReader relOptSchema = createCatalogReader(false);
    return new SduCalciteRelBuilder(frameworkConfig.getContext(), cluster, relOptSchema);
  }

  Context getContext() {
    return context;
  }

  RelOptPlanner getPlanner() {
    return planner;
  }
}
