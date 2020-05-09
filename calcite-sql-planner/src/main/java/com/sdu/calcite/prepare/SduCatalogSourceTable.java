package com.sdu.calcite.prepare;

import static java.lang.String.format;

import com.sdu.calcite.plan.SduCalciteRelBuilder;
import com.sdu.calcite.plan.SduContext;
import com.sdu.calcite.plan.SduSqlExprToRexConverterFactory;
import com.sdu.calcite.plan.catalog.SduCalciteTable;
import com.sdu.calcite.plan.catalog.SduCatalogTableColumn;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

public class SduCatalogSourceTable extends SduPreparingTable {

  private final SduCalciteTable calciteTable;

  public SduCatalogSourceTable(
      RelOptSchema relOptSchema,
      RelDataType rowType,
      List<String> names,
      SduCalciteTable calciteTable) {
    super(relOptSchema, rowType, names, calciteTable.getStatistic());
    this.calciteTable = calciteTable;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    // TableScanRule 规则触发
    Map<String, String> columnToExpression = calciteTable.getCatalogTable()
        .getColumns()
        .stream()
        .filter(column -> column.getExpr().isPresent())
        .collect(Collectors.toMap(SduCatalogTableColumn::getName, column -> column.getExpr().get()));

    RelOptCluster cluster = context.getCluster();

    /*
     * RelNode:
     *
     *  LogicalProject(physical column + computed column)
     *        TableScan(physical column)
     * */

    // physical column
    Integer[] physicalColumnIndexes = getRowType().getFieldList()
        .stream()
        .filter(field -> !columnToExpression.containsKey(field.getName()))
        .map(RelDataTypeField::getIndex)
        .toArray(Integer[]::new);

    RelOptTable newRelTable = SduTableSourceTable.of(this, physicalColumnIndexes);
    LogicalTableScan tableScan = LogicalTableScan.create(cluster, newRelTable);

    SduCalciteRelBuilder relBuilder = SduCalciteRelBuilder.of(cluster, getRelOptSchema());

    // Push TableScan
    relBuilder.push(tableScan);

    // Push Project
    if (!columnToExpression.isEmpty()) {
      // 虚拟列放在Project中(SQL关键字处理)
      String[] totalColumns = getRowType().getFieldNames()
          .stream()
          .map(field -> columnToExpression.getOrDefault(field, format("`%s`", field)))
          .toArray(String[]::new);

      SduContext ctx = cluster.getPlanner().getContext().unwrap(SduContext.class);
      SduSqlExprToRexConverterFactory toRexFactory = ctx.getSqlExprToRexConverterFactory();
      RexNode[] rexNodes = toRexFactory.create(newRelTable.getRowType()).convertToRexNodes(totalColumns);
      relBuilder.project(rexNodes);
    }

    return relBuilder.build();
  }

}
