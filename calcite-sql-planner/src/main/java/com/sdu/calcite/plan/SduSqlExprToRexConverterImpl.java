package com.sdu.calcite.plan;

import static java.util.Objects.requireNonNull;

import com.sdu.calcite.CalciteSchemaBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;

class SduSqlExprToRexConverterImpl implements SduSqlExprToRexConverter {

  private static final String TEMPORARY_TABLE_NAME = "__temp_table__";
  private static final String QUERY_FORMAT = "SELECT %s FROM " + TEMPORARY_TABLE_NAME;

  private final FrameworkConfig frameworkConfig;
  private final SduSqlPlanner planner;

  SduSqlExprToRexConverterImpl(
      FrameworkConfig frameworkConfig,
      SduCalciteTypeFactory typeFactory,
      RelOptCluster cluster,
      RelDataType tableRowType) {
    this.frameworkConfig = requireNonNull(frameworkConfig);

    /*
     * SQL语法验证:
     *
     * 1: CalciteCatalogReader
     *
     *    Table查询
     *
     * 2: SqlOperatorTable
     *
     *    SqlFunction查询, 这里通过FrameworkConfig将SqlOperatorTable传给Planner
     * */
    this.planner = new SduSqlPlanner(
        frameworkConfig,
        (isLenient) -> createSingleTableCatalogReader(isLenient, frameworkConfig, typeFactory, tableRowType),
        typeFactory,
        cluster)
    ;
  }

  @Override
  public RexNode convertToRexNode(String expr) {
    return convertToRexNodes(new String[]{expr})[0];
  }

  @Override
  public RexNode[] convertToRexNodes(String[] expressions) {
    try {
      String query = String.format(QUERY_FORMAT, String.join(",", expressions));
      SqlParser parser = SqlParser.create(query, frameworkConfig.getParserConfig());
      // 解析
      SqlNode parsed = parser.parseStmt();
      // 验证
      SqlNode validated = planner.validate(parsed);
      // ToRel
      RelNode rel = planner.rel(validated).rel;
      // The plan should in the following tree
      // LogicalProject
      // +- TableScan
      if (rel instanceof LogicalProject
          && rel.getInput(0) != null
          && rel.getInput(0) instanceof TableScan) {
        return ((LogicalProject) rel).getProjects().toArray(new RexNode[0]);
      } else {
        throw new IllegalStateException("The root RelNode should be LogicalProject, but is " + rel.toString());
      }
    } catch (Exception e) {
      // ignore
      e.printStackTrace();
    }


    return new RexNode[0];
  }


  private static CalciteCatalogReader createSingleTableCatalogReader(
      boolean lenientCaseSensitivity,
      FrameworkConfig config,
      SduCalciteTypeFactory typeFactory,
      RelDataType rowType) {
    // connection properties
    boolean caseSensitive = !lenientCaseSensitivity && config.getParserConfig().caseSensitive();
    Properties properties = new Properties();
    properties.put(
        CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(caseSensitive));
    CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);

    // prepare root schema
    final RowTypeSpecifiedTable table = new RowTypeSpecifiedTable(rowType);
    final Map<String, Table> tableMap = Collections.singletonMap(TEMPORARY_TABLE_NAME, table);
    CalciteSchema schema = CalciteSchemaBuilder.asRootSchema(new TableSpecifiedSchema(tableMap));

    return new SduCalciteCatalogReader(
        schema,
        new ArrayList<>(new ArrayList<>()),
        typeFactory,
        connectionConfig);
  }

  private static class RowTypeSpecifiedTable extends AbstractTable {

    private final RelDataType rowType;

    RowTypeSpecifiedTable(RelDataType rowType) {
      this.rowType = rowType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType;
    }

  }

  private static class TableSpecifiedSchema extends AbstractSchema {

    private Map<String, Table> tables;

    TableSpecifiedSchema(Map<String, Table> tables) {
      this.tables = tables;
    }

    @Override
    protected Map<String, Table> getTableMap() {
      return tables;
    }
  }
}
