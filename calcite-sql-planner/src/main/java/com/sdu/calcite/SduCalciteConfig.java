package com.sdu.calcite;

import com.sdu.sql.parse.SduFunctionCatalog;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public interface SduCalciteConfig {

  Optional<CalciteSchema> getCalciteSchema();

  Optional<SduFunctionCatalog> getFunctionCatalog();

  // SQL解析配置参数
  Optional<SqlParser.Config> getSqlParserConfig();

  // RelNode优化参数载体
  Optional<Context> getContext();

  // SqlNode转RelNode配置参数
  Optional<SqlToRelConverter.Config> getSqlToRelConvertConfig();

  // RelNode默认特征属性
  Optional<List<RelTraitDef>> getDefaultRelTrait();

  // RelNode代价
  Optional<RelOptCostFactory> getRelOptCostFactory();

  Optional<RelDataTypeFactory> getRelDataTypeFactory();

}
