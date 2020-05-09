package com.sdu.calcite.prepare;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;

public abstract class SduPreparingTable extends Prepare.AbstractPreparingTable {

  private static final double DEFAULT_ROWCOUNT = 1E8;

  private final RelOptSchema relOptSchema;
  private final RelDataType rowType;
  private final List<String> names;
  private final Statistic statistic;

  SduPreparingTable(
      RelOptSchema relOptSchema,
      RelDataType rowType,
      List<String> names,
      Statistic statistic) {
    this.relOptSchema = relOptSchema;
    this.rowType = requireNonNull(rowType);
    this.names = requireNonNull(ImmutableList.copyOf(names));
    this.statistic = requireNonNull(statistic);
  }

  Statistic getStatistic() {
    return statistic;
  }

  @Override
  protected RelOptTable extend(Table extendedTable) {
    throw new RuntimeException("Extending column not supported");
  }

  @Override
  public List<String> getQualifiedName() {
    return names;
  }

  @Override
  public SqlMonotonicity getMonotonicity(String columnName) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }

  @Override
  public boolean supportsModality(SqlModality modality) {
    return false;
  }

  @Override
  public boolean isTemporal() {
    return true;
  }

  @Override
  public double getRowCount() {
    Double rowCnt = statistic.getRowCount();
    return rowCnt == null ? DEFAULT_ROWCOUNT : rowCnt;
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return relOptSchema;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return LogicalTableScan.create(context.getCluster(), this);
  }

  @Override
  public List<RelCollation> getCollationList() {
    return ImmutableList.of();
  }

  @Override
  public RelDistribution getDistribution() {
    return null;
  }

  @Override
  public boolean isKey(ImmutableBitSet columns) {
    return false;
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return ImmutableList.of();
  }

  @Override
  public Expression getExpression(Class clazz) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return null;
  }

}
