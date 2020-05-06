package com.sdu.calcite.plan;

import java.util.List;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;

public class SduCalciteSqlPlanner implements ViewExpander {

  private final FrameworkConfig frameworkConfig;
  private final RelOptPlanner planner;
  private final Function<Boolean, CalciteCatalogReader> catalogReaderSupplier;
  private final RelDataTypeFactory typeFactory;

  private SduCalciteSqlValidator validator;

  SduCalciteSqlPlanner(
      FrameworkConfig frameworkConfig,
      Function<Boolean, CalciteCatalogReader> catalogReaderSupplier,
      RelOptPlanner planner,
      RelDataTypeFactory typeFactory) {
    this.planner = planner;
    this.typeFactory = typeFactory;
    this.frameworkConfig = frameworkConfig;
    this.catalogReaderSupplier = catalogReaderSupplier;
    // 指定构建RelNode时添加的默认特征
    for (RelTraitDef traitDef : frameworkConfig.getTraitDefs()) {
      planner.addRelTraitDef(traitDef);
    }
  }

  public SqlNode parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
    return parser.parseQuery(sql);
  }

  public SqlNode validate(SqlNode sqlNode) {
    validator = getOrCreateSqlValidator();

    // DDL不需要验证
    if (sqlNode.getKind().belongsTo(SqlKind.DDL)
        || sqlNode.getKind() == SqlKind.CREATE_FUNCTION
        || sqlNode.getKind() == SqlKind.DROP_FUNCTION
        || sqlNode.getKind() == SqlKind.OTHER_DDL) {
      return sqlNode;
    }

    return validator.validate(sqlNode);
  }

  public RelRoot validateAndRel(SqlNode sqlNode) {
    validator = getOrCreateSqlValidator();
    assert validator != null;
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = SduCalciteRelOptClusterFactory.create(planner, rexBuilder);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
        this,
        validator,
        validator.getCatalogReader().unwrap(CatalogReader.class),
        cluster,
        frameworkConfig.getConvertletTable(),
        frameworkConfig.getSqlToRelConverterConfig());
    return sqlToRelConverter.convertQuery(sqlNode, true, true);
  }

  public SduCalciteSqlValidator getOrCreateSqlValidator() {
    if (validator == null) {
      CalciteCatalogReader catalogReader = catalogReaderSupplier.apply(false);
      validator = new SduCalciteSqlValidator(frameworkConfig.getOperatorTable(),
          catalogReader, typeFactory, frameworkConfig.getParserConfig().conformance());
      validator.setIdentifierExpansion(true);
    }
    return validator;
  }

  @Override
  public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
      List<String> viewPath) {
    throw new RuntimeException("");
  }

}
