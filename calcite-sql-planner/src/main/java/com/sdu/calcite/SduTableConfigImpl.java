package com.sdu.calcite;

import com.sdu.calcite.api.SduTableConfig;
import com.sdu.calcite.plan.SduRelOptimizerFactory;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SduTableConfigImpl implements SduTableConfig {

  private final boolean replacesSqlOperatorTable;
  private final Optional<SqlOperatorTable> operatorTable;
  private final Optional<SqlParser.Config> sqlParserConfig;
  private final Optional<SqlToRelConverter.Config> sqlToRelConvertConfig;
  private final Optional<List<RelTraitDef>> traitDefs;
  private final Optional<RelOptCostFactory> costFactory;
  private final Optional<SduRelOptimizerFactory> optimizerFactory;

  public SduTableConfigImpl(
      boolean replacesSqlOperatorTable,
      SqlOperatorTable operatorTable,
      Config sqlParserConfig,
      SqlToRelConverter.Config sqlToRelConvertConfig,
      List<RelTraitDef> traitDefs,
      RelOptCostFactory costFactory,
      SduRelOptimizerFactory optimizerFactory) {
    this.replacesSqlOperatorTable = replacesSqlOperatorTable;
    this.operatorTable = Optional.ofNullable(operatorTable);
    this.sqlParserConfig = Optional.ofNullable(sqlParserConfig);
    this.sqlToRelConvertConfig = Optional.ofNullable(sqlToRelConvertConfig);
    this.traitDefs = Optional.ofNullable(traitDefs);
    this.costFactory = Optional.ofNullable(costFactory);
    this.optimizerFactory = Optional.ofNullable(optimizerFactory);
  }

  @Override
  public boolean replacesSqlOperatorTable() {
    return replacesSqlOperatorTable;
  }

  @Override
  public Optional<SqlOperatorTable> getSqlOperatorTable() {
    return operatorTable;
  }

  @Override
  public Optional<Config> getSqlParserConfig() {
    return sqlParserConfig;
  }

  @Override
  public Optional<SqlToRelConverter.Config> getSqlToRelConvertConfig() {
    return sqlToRelConvertConfig;
  }

  @Override
  public Optional<List<RelTraitDef>> getTraitDefs() {
    return traitDefs;
  }

  @Override
  public Optional<RelOptCostFactory> getCostFactory() {
    return costFactory;
  }

  @Override
  public Optional<SduRelOptimizerFactory> getOptimizerFactory() {
    return optimizerFactory;
  }


}
