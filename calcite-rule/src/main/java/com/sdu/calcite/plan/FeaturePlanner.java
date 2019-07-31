package com.sdu.calcite.plan;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelConversionException;

import com.sdu.calcite.utils.CalciteSqlUtils;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class FeaturePlanner {

  private final FrameworkConfig frameworkConfig;

  private final RelOptPlanner planner;

  private final RelDataTypeFactory typeFactory;

  // 校验SQL
  private FeatureSqlValidator sqlValidator;

  public FeaturePlanner(FrameworkConfig frameworkConfig, RelOptPlanner planner, RelDataTypeFactory typeFactory) {
    this.frameworkConfig = frameworkConfig;
    this.planner = planner;
    this.typeFactory = typeFactory;
  }

  public SqlNode parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
    return parser.parseStmt();
  }

  public SqlNode validate(SqlNode sqlNode) throws SqlParseException {
    sqlValidator = new FeatureSqlValidator(
        frameworkConfig.getOperatorTable(), createCatalogReader() , typeFactory);
    sqlValidator.setIdentifierExpansion(true);

    return sqlValidator.validate(sqlNode);
  }

  public RelRoot rel(SqlNode sqlNode) throws RelConversionException {
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = CalciteSqlUtils.createRelOptCluster(planner, rexBuilder);

    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
        new ViewExpanderImpl(),
        sqlValidator,
        createCatalogReader(),
        cluster,
        frameworkConfig.getConvertletTable(),
        frameworkConfig.getSqlToRelConverterConfig());

    return sqlToRelConverter.convertQuery(sqlNode, false, true);
  }

  private CalciteCatalogReader createCatalogReader() {
    return CalciteSqlUtils.createCatalogReader(frameworkConfig.getDefaultSchema(), typeFactory, frameworkConfig.getParserConfig());
  }

  private static class ViewExpanderImpl implements RelOptTable.ViewExpander {

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
      return null;
    }

  }

}
