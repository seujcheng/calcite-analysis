package com.sdu.calcite.plan;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.config.CalciteConnectionProperty.CASE_SENSITIVE;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.SduInternalFunctionTable;
import com.sdu.calcite.api.SduTableConfig;
import com.sdu.calcite.api.SqlParserException;
import com.sdu.calcite.plan.catalog.SduCatalogFunctionOperatorTable;
import com.sdu.calcite.plan.catalog.SduFunctionCatalog;
import com.sdu.calcite.plan.cost.SduRelOptCostFactory;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class SduPlannerContext {

  private final RelOptCostFactory costFactory;
  private final RelDataTypeSystem typeSystem;
  private final SduCalciteTypeFactory typeFactory;

  private SduTableConfig tableConfig;
  private SduFunctionCatalog functionCatalog;

  private final SduContext context;
  private final RelOptCluster cluster;
  private final CalciteSchema rootSchema;
  private final RelOptPlanner planner;


  SduPlannerContext(
      SduTableConfig tableConfig,
      SduFunctionCatalog functionCatalog,
      CalciteSchema rootSchema) {
    this.tableConfig = tableConfig;
    this.functionCatalog = functionCatalog;
    this.rootSchema = rootSchema;

    this.context = new SduContextImpl(tableConfig, this::createSqlExprToRexConverter);

    this.typeSystem = RelDataTypeSystem.DEFAULT;
    this.typeFactory = new SduCalciteTypeFactory(typeSystem);

    this.costFactory = tableConfig.getCostFactory().orElse(new SduRelOptCostFactory());
    this.planner = new VolcanoPlanner(costFactory, this.context);
    tableConfig.getTraitDefs()
        .ifPresent(traitDefs -> {
          for (RelTraitDef traitDef : traitDefs) {
            planner.addRelTraitDef(traitDef);
          }
        });
    this.cluster = SduCalciteRelOptClusterFactory.create(planner, new RexBuilder(typeFactory));
  }

  private SduSqlExprToRexConverter createSqlExprToRexConverter(RelDataType rowType) {
    return new SduSqlExprToRexConverterImpl(
        createFrameworkConfig(),
        typeFactory,
        cluster,
        rowType);
  }

  private SqlParser.Config getSqlParserConfig() {
    return tableConfig.getSqlParserConfig()
        .orElseGet(() ->
          SqlParser.configBuilder()
              .setQuoting(Quoting.BACK_TICK)
//              .setLex(Lex.JAVA)
              // 禁止转为大写
              .setUnquotedCasing(Casing.UNCHANGED)
              .setParserFactory(new SduCalciteSqlParserFactory())
              .build()
        );
  }

  private SqlToRelConverter.Config getSqlToRelConverterConfig() {
    return tableConfig.getSqlToRelConvertConfig()
        .orElseGet(() ->
            SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                // TableScan --> LogicalTableScan(即: TableScan类型是LogicalTableScan)
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .withRelBuilderFactory(new SduCalciteRelBuilderFactory(context))
                .build()
        );
  }

  private SqlOperatorTable getBuiltinSqlOperatorTable() {
    return ChainedSqlOperatorTable.of(new SduInternalFunctionTable(),
        new SduCatalogFunctionOperatorTable(typeFactory, functionCatalog));
  }

  private SqlOperatorTable getSqlOperatorTable() {
    return tableConfig.getSqlOperatorTable()
        .map(operatorTable -> {
          if (tableConfig.replacesSqlOperatorTable()) {
            return operatorTable;
          }
          return ChainedSqlOperatorTable.of(getBuiltinSqlOperatorTable(), operatorTable);
        })
        .orElseGet(this::getBuiltinSqlOperatorTable);

  }

  private FrameworkConfig createFrameworkConfig() {
    // 构建RelNode默认特征
    List<RelTraitDef> traitDefs = tableConfig.getTraitDefs()
        .orElse(ImmutableList.of(ConventionTraitDef.INSTANCE));

    return Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema.plus())
        .parserConfig(getSqlParserConfig())
        .costFactory(costFactory)
        .typeSystem(typeSystem)
        .operatorTable(getSqlOperatorTable())
        .sqlToRelConverterConfig(getSqlToRelConverterConfig())
        // TODO: executor
        .context(context)
        .traitDefs(traitDefs)
        .build();
  }

  private SchemaPlus getRootSchema(SchemaPlus schema) {
    if (schema.getParentSchema() == null) {
      return schema;
    }
    return getRootSchema(schema.getParentSchema());
  }

  private CalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity,
      String currentCatalog, String currentDatabase) {
    // SqlNameMatcher是否区分大小写, 具体实现: BaseMatcher
    SqlParser.Config parserConfig = getSqlParserConfig();
    boolean caseSensitive;
    if (lenientCaseSensitivity) {
      caseSensitive = false;
    } else {
      caseSensitive = parserConfig.caseSensitive();
    }
    Properties props = new Properties();
    props.setProperty(CASE_SENSITIVE.camelName(), valueOf(caseSensitive));
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

    SchemaPlus rootSchema = getRootSchema(this.rootSchema.plus());
    return new SduCalciteCatalogReader(
        CalciteSchema.from(rootSchema),
        /*
         * 元数据分三级管理: catalog <-- database <-- table
         *
         * 描述方式: catalog, catalog.database
         * */
        asList(
            asList(currentCatalog, currentDatabase),
            singletonList(currentCatalog)
        ),
        typeFactory,
        config
    );
  }

  public SqlNodeList parseStmtList(String sql) {
    try {
      SqlParser parser = SqlParser.create(sql, getSqlParserConfig());
      return parser.parseStmtList();
    } catch (SqlParseException e) {
      throw new SqlParserException("failed parse sql", e);
    }
  }

  public SqlNode parseStmt(String sql) {
    try {
      SqlParser parser = SqlParser.create(sql, getSqlParserConfig());
      return parser.parseStmt();
    } catch (SqlParseException e) {
      throw new SqlParserException("failed parse sql", e);
    }
  }

  SduSqlPlanner createPlanner(String currentCatalog, String currentDatabase) {
    return new SduSqlPlanner(
        createFrameworkConfig(),
        isLenient -> createCatalogReader(isLenient, currentCatalog, currentDatabase),
        typeFactory,
        cluster);
  }
}
