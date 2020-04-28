package com.sdu.calcite;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.catelog.SduCalciteTable;
import com.sdu.calcite.plan.SduCalciteTypeFactory;
import com.sdu.sql.entry.SduSqlStatement;
import com.sdu.sql.entry.SduTable;
import com.sdu.sql.parse.SduFunctionCatalog;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SduCalciteConfigImpl implements SduCalciteConfig{

  private final CalciteSchema calciteSchema;
  private final SduFunctionCatalog functionCatalog;

  private SduCalciteConfigImpl(CalciteSchema calciteSchema, SduFunctionCatalog functionCatalog) {
    this.calciteSchema = calciteSchema;
    this.functionCatalog = functionCatalog;
  }

  @Override
  public Optional<CalciteSchema> getCalciteSchema() {
    return Optional.ofNullable(calciteSchema);
  }

  @Override
  public Optional<SduFunctionCatalog> getFunctionCatalog() {
    return Optional.ofNullable(functionCatalog);
  }

  @Override
  public Optional<Config> getSqlParserConfig() {
    return Optional.empty();
  }

  @Override
  public Optional<Context> getContext() {
    return Optional.empty();
  }

  @Override
  public Optional<SqlToRelConverter.Config> getSqlToRelConvertConfig() {
    return Optional.empty();
  }

  @Override
  public Optional<List<RelTraitDef>> getDefaultRelTrait() {
    return Optional.of(ImmutableList.of(ConventionTraitDef.INSTANCE));
  }

  @Override
  public Optional<RelOptCostFactory> getRelOptCostFactory() {
    return Optional.empty();
  }

  @Override
  public Optional<RelDataTypeFactory> getRelDataTypeFactory() {
    return Optional.of(new SduCalciteTypeFactory());
  }

  public static SduCalciteConfig fromSduSqlStatement(SduSqlStatement statement) {
    Objects.requireNonNull(statement);
    Objects.requireNonNull(statement.getTables());

    CalciteSchema schema = CalciteSchema.createRootSchema(false, false);
    for (SduTable table : statement.getTables()) {
      schema.add(table.getName(), new SduCalciteTable(table.getColumns(), table.getProperties()));
    }

    return new SduCalciteConfigImpl(schema, statement.getFunctionCatalog());
  }

}
