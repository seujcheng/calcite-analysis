package com.sdu.calcite.plan.nodes.logical;

import com.sdu.calcite.plan.nodes.SduLogicalRel;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;

/*
 * AbstractRelNode子类需实现的方法:
 *
 * 1: computeSelfCost()
 *
 *    计算节点代价时, 该方法被触发
 *
 * 2: copy()
 *
 *    RelNode转为另一种RelNode时, 该方法被调用
 * */
public class SduLogicalTableScan extends TableScan implements SduLogicalRel {

  private final Optional<List<Integer>> selectedFields;

  public SduLogicalTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Optional<List<Integer>> selectedFields) {
    super(cluster, traitSet, table);
    this.selectedFields = selectedFields;
  }

  public SduLogicalTableScan copy(RelTraitSet traitSet, List<RelNode> inputs, Optional<List<Integer>> selectedFields) {
    return new SduLogicalTableScan(getCluster(),
        traitSet,
        table,
        selectedFields);
  }

  @Override
  public RelDataType deriveRowType() {
    RelDataType baseRowType = table.getRowType();
    /*
     * 谓词下推: 减少数据的读取
     * */
    return selectedFields.map(selectedFieldIndexes -> {
      List<RelDataTypeField> fields = baseRowType.getFieldList();
      FieldInfoBuilder builder = getCluster().getTypeFactory().builder();
      selectedFieldIndexes.stream().map(fields::get).forEach(builder::add);
      return builder.build();
    }).orElse(baseRowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    /*
     * 1: VolcanoPlanner
     *
     * 2: getRowType()调用deriveRowType(), 默认deriveRowType()未实现
     * */
    double rowCnt = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCnt,
        rowCnt,
        rowCnt * estimateRowSize(getRowType()));
  }

}
