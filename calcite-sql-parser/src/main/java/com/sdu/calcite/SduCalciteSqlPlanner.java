package com.sdu.calcite;

import java.util.List;
import java.util.Properties;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;

public class SduCalciteSqlPlanner {

  private final FrameworkConfig frameworkConfig;
  @Getter
  private final SduCalciteRelBuilder builder;
  private final RelDataTypeFactory typeFactory;

  private SduCalciteSqlValidator validator;

  SduCalciteSqlPlanner(FrameworkConfig frameworkConfig, SduCalciteRelBuilder builder,
      RelDataTypeFactory typeFactory) {
    this.frameworkConfig = frameworkConfig;
    this.builder = builder;
    this.typeFactory = typeFactory;
  }

  public SqlNode parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
    return parser.parseQuery(sql);
  }

  public RelRoot validateAndRel(SqlNode sqlNode) {
    CalciteCatalogReader catalogReader = createCatalogReader(frameworkConfig.getDefaultSchema(),
        typeFactory,
        frameworkConfig.getParserConfig());

    // Validate SqlNode
    validator = new SduCalciteSqlValidator(frameworkConfig.getOperatorTable(),
        catalogReader, typeFactory, frameworkConfig.getParserConfig().conformance());

    // Translate To RelNode
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(builder.getPlaner(), rexBuilder);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
        new ViewExpanderImpl(),
        validator,
        catalogReader,
        cluster,
        frameworkConfig.getConvertletTable(),
        frameworkConfig.getSqlToRelConverterConfig());

    return sqlToRelConverter.convertQuery(sqlNode, true, true);
  }



  private static class ViewExpanderImpl implements RelOptTable.ViewExpander {

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
        List<String> viewPath) {
      return null;
    }

  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    if (schema.getParentSchema() == null) {
      return schema;
    }
    return rootSchema(schema.getParentSchema());
  }

  private static CalciteCatalogReader createCatalogReader(SchemaPlus defaultSchema,
      RelDataTypeFactory typeFactory, SqlParser.Config parserConfig) {
    SchemaPlus rootSchema = rootSchema(defaultSchema);

    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(parserConfig.caseSensitive()));

    return new CalciteCatalogReader(
        CalciteSchema.from(rootSchema),
        CalciteSchema.from(defaultSchema).path(null),
        typeFactory,
        new CalciteConnectionConfigImpl(props));
  }

}
