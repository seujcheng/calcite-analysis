package com.sdu.calcite.plan.nodes.logical;

import com.sdu.calcite.plan.exec.DataSource;
import com.sdu.calcite.plan.exec.DataTransformation;
import com.sdu.calcite.plan.nodes.SduExecuteRel;
import com.sdu.calcite.plan.nodes.SduLogicalRel;
import com.sdu.calcite.table.data.SduGenericRowData;
import com.sdu.calcite.table.types.SduBigIntType;
import com.sdu.calcite.table.types.SduIntType;
import com.sdu.calcite.table.types.SduRowType;
import com.sdu.calcite.table.types.SduRowType.SduRowField;
import com.sdu.calcite.table.types.SduVarCharType;
import java.util.Arrays;
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
public class SduLogicalTableScan extends TableScan implements SduLogicalRel, SduExecuteRel {

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

  @Override
  public DataTransformation translateToPlanInternal() {
    // TODO: 读取数据, 这里暂时写死
    List<SduRowField> fields = Arrays.asList(
        new SduRowField("id", new SduBigIntType(), "ID"),
        new SduRowField("uname", new SduVarCharType(100), "NAME"),
        new SduRowField("age", new SduIntType(), "AGE"),
        new SduRowField("phone", new SduBigIntType(), "PHONE"),
        new SduRowField("address", new SduVarCharType(1000), "ADDRESS")
    );
    SduRowType inputType = new SduRowType(true, fields);

    SduGenericRowData data01 = new SduGenericRowData(5);
    data01.setField(0, 10001);
    data01.setField(1, "张小龙");
    data01.setField(2, 18);
    data01.setField(3, 13567834567L);
    data01.setField(4, "北京市朝阳区望京花园");

    SduGenericRowData data02 = new SduGenericRowData(5);
    data02.setField(0, 10002);
    data02.setField(1, "王晓飞");
    data02.setField(2, 23);
    data02.setField(3, 13699870932L);
    data02.setField(4, "北京市海淀区上地东里");

    SduGenericRowData data03 = new SduGenericRowData(5);
    data03.setField(0, 10003);
    data03.setField(1, "李萌萌");
    data03.setField(2, 33);
    data03.setField(3, 13933870932L);
    data03.setField(4, "北京市昌平区华域金府");

    return new DataSource(inputType, Arrays.asList(data01, data02, data03));
  }

}
