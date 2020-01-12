package com.sdu.calcite.sql;

import com.sdu.calcite.sql.ddl.SqlCreateTable;
import com.sdu.calcite.sql.ddl.SqlTableColumn;
import com.sdu.calcite.sql.ddl.SqlUseFunction;
import com.sdu.calcite.sql.planner.XSqlParserImplFactory;
import com.sdu.calcite.sql.planner.XSqlPlanner;
import com.sdu.calcite.sql.table.XTable;
import com.sdu.calcite.sql.table.XTableColumn;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class XCalciteUtils {

  private XCalciteUtils() {

  }

  private static CalciteSchema createCalciteSchema(List<SqlCreateTable> createTables, RelDataTypeFactory typeFactory) {
    CalciteSchema schema = CalciteSchema.createRootSchema(false, false);

    for (SqlCreateTable table : createTables) {
      List<XTableColumn> columns = new LinkedList<>();
      for (SqlNode column : table.getColumnList()) {
        SqlTableColumn c = (SqlTableColumn) column;
        columns.add(new XTableColumn(c.getName(), c.getType(typeFactory).getSqlTypeName(),
            c.getComment()));
      }
      schema.add(table.getTableName(), new XTable(columns));
    }

    return schema;
  }

  private static SqlOperatorTable createSqlOperatorTable(XTypeFactory typeFactory, List<SqlUseFunction> functions) {
    return ChainedSqlOperatorTable.of(new XBasicOperatorTable(),
        new XUserFunctionOperatorTable(typeFactory, functions));
  }


  private static SqlParser.Config createSqlParserConfig() {
    return SqlParser.configBuilder()
        // 禁止转为大写
        .setUnquotedCasing(Casing.UNCHANGED)
        .setParserFactory(new XSqlParserImplFactory())
        .setLex(Lex.JAVA)
        .build();
  }

  private static SqlToRelConverter.Config createSqlToRelConverterConfig() {
    return SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        // TableScan --> LogicalTableScan(即: TableScan类型是LogicalTableScan)
        .withConvertTableAccess(false)
        .withInSubQueryThreshold(Integer.MAX_VALUE)
        .build();
  }

  private static FrameworkConfig createFrameworkConfig(XTypeFactory typeFactory,
      List<SqlCreateTable> createTables, List<SqlUseFunction> functions) {
    // register table schema
    CalciteSchema schema = createCalciteSchema(createTables, typeFactory);
    // register function schema
    SqlOperatorTable operatorTable = createSqlOperatorTable(typeFactory, functions);
    return Frameworks.newConfigBuilder()
        .defaultSchema(schema.plus())
        .parserConfig(createSqlParserConfig())
        .typeSystem(typeFactory.getTypeSystem())
        .operatorTable(operatorTable)
        .sqlToRelConverterConfig(createSqlToRelConverterConfig())
        .build();
  }

  /**
   * The method use to obtain schema, such as table, function and so on.
   * */
  private static CalciteCatalogReader createCatalogReader(
      CalciteSchema rootSchema, List<String> defaultSchema, RelDataTypeFactory typeFactory,
      SqlParser.Config parserConfig) {
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(parserConfig.caseSensitive()));

    return new CalciteCatalogReader(
        rootSchema, defaultSchema, typeFactory, new CalciteConnectionConfigImpl(props));
  }

  private static XRelBuilder createRelBuilder(FrameworkConfig frameworkConfig, XTypeFactory typeFactory) {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    CalciteSchema calciteSchema = CalciteSchema.from(frameworkConfig.getDefaultSchema());
    CalciteCatalogReader relOptSchema = createCatalogReader(
        calciteSchema, Collections.emptyList(), typeFactory,
        frameworkConfig.getParserConfig());
    return new XRelBuilder(frameworkConfig.getContext(), cluster, relOptSchema);

  }

  public static XSqlPlanner createXSqlPlanner(List<SqlCreateTable> createTables, List<SqlUseFunction> functions) {
    XTypeFactory typeFactory = new XTypeFactory(RelDataTypeSystem.DEFAULT);
    FrameworkConfig config = createFrameworkConfig(typeFactory, createTables, functions);
    XRelBuilder relBuilder = createRelBuilder(config, typeFactory);

    return new XSqlPlanner(config, relBuilder.getPlaner(), typeFactory);
  }

}
