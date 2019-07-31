package com.sdu.calcite.plan;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import com.sdu.calcite.utils.CalciteSqlUtils;

/**
 * @author hanhan.zhang
 * */
public class FeatureTableEnvironment {

  // 元数据
  private final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, true).plus();

  //
  private final FunctionCatalog functionCatalog = FunctionCatalog.withBuiltIns();


  private FrameworkConfig frameworkConfig;
  private FeatureRelBuilder relBuilder;


  public void registerFunction(SqlFunction sqlFunction) {
    functionCatalog.registerSqlFunction(sqlFunction);
  }

  public void registerTable(String name, Table table) {
    rootSchema.add(name, table);
  }

  public FrameworkConfig frameworkConfig() {
    if (frameworkConfig != null) {
      return frameworkConfig;
    }

    synchronized (this) {
      if (frameworkConfig == null) {
        frameworkConfig = Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            // TODO: CostFactory
            .parserConfig(getSqlParserConfig())
            // TODO:
            .typeSystem(RelDataTypeSystem.DEFAULT)
            .operatorTable(getSqlOperatorTable())
            .sqlToRelConverterConfig(getSqlToRelConverterConfig())
            // TODO: Executor
            .build();

        relBuilder = CalciteSqlUtils.createRelBuilder(frameworkConfig);
      }
    }

    return frameworkConfig;
  }

  public RelOptPlanner getPlanner() {
    return relBuilder.getPlaner();
  }

  public RelDataTypeFactory getTypeFactory() {
    return relBuilder.getTypeFactory();
  }

  private SqlOperatorTable getSqlOperatorTable() {
    return functionCatalog.getSqlOperatorTable();
  }

  private static SqlParser.Config getSqlParserConfig() {
    return SqlParser.configBuilder().setLex(Lex.JAVA).build();
  }

  private static SqlToRelConverter.Config getSqlToRelConverterConfig() {
    return SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        // TableScan --> LogicalTableScan(即: TableScan类型是LogicalTableScan)
        .withConvertTableAccess(false)
        .withInSubQueryThreshold(Integer.MAX_VALUE)
        .build();
  }

}
