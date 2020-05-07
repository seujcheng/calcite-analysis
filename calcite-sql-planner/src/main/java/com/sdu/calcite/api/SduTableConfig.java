package com.sdu.calcite.api;

import com.sdu.calcite.plan.SduRelOptimizerFactory;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public interface SduTableConfig {

  boolean replacesSqlOperatorTable();

  Optional<SqlOperatorTable> getSqlOperatorTable();

  // SQL解析配置参数
  Optional<SqlParser.Config> getSqlParserConfig();

  // SqlNode转RelNode配置参数
  Optional<SqlToRelConverter.Config> getSqlToRelConvertConfig();

  // RelNode默认特征属性
  Optional<List<RelTraitDef>> getTraitDefs();

  // RelNode代价
  Optional<RelOptCostFactory> getCostFactory();

  Optional<SduRelOptimizerFactory> getOptimizerFactory();

}
