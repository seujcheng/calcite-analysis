package com.sdu.calcite.plan;

import java.util.List;
import java.util.function.Function;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;

public class SduSqlPlanner {

  private final FrameworkConfig frameworkConfig;
  private final SduCalciteTypeFactory typeFactory;
  private final RelOptCluster cluster;
  private final Function<Boolean, CalciteCatalogReader> catalogReaderSupplier;

  private SduSqlValidator validator;

  SduSqlPlanner(
      FrameworkConfig frameworkConfig,
      Function<Boolean, CalciteCatalogReader> catalogReaderSupplier,
      SduCalciteTypeFactory typeFactory,
      RelOptCluster cluster) {
    this.typeFactory = typeFactory;
    this.frameworkConfig = frameworkConfig;
    this.catalogReaderSupplier = catalogReaderSupplier;
    this.cluster = cluster;
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

  public RelRoot rel(SqlNode sqlNode) {
    return rel(sqlNode, getOrCreateSqlValidator());
  }

  private RelRoot rel(SqlNode sqlNode, SduSqlValidator validator) {
    assert validator != null;
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
        new ToRelContextImpl(cluster),
        validator,
        validator.getCatalogReader().unwrap(CatalogReader.class),
        cluster,
        frameworkConfig.getConvertletTable(),
        frameworkConfig.getSqlToRelConverterConfig());
    return sqlToRelConverter.convertQuery(sqlNode, true, true);
  }

  public SduSqlValidator getOrCreateSqlValidator() {
    if (validator == null) {
      CalciteCatalogReader catalogReader = catalogReaderSupplier.apply(false);
      validator = new SduSqlValidator(
          frameworkConfig.getOperatorTable(),
          catalogReader,
          typeFactory);
      validator.setIdentifierExpansion(true);
      validator.setDefaultNullCollation(NullCollation.LOW);
    }
    return validator;
  }

  public FrameworkConfig config() {
    return frameworkConfig;
  }

  class ToRelContextImpl implements RelOptTable.ToRelContext {

    final RelOptCluster cluster;

    ToRelContextImpl(RelOptCluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public RelOptCluster getCluster() {
      return cluster;
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
        List<String> viewPath) {
      return null;
    }
  }

}
