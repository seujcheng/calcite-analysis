package com.sdu.calcite.plan.nodes.logical;

import static com.sdu.calcite.plan.codegen.SduCalcCodeGenerator.generateCalcTransformation;
import static com.sdu.calcite.plan.codegen.SduCodeGenUtils.DEFAULT_INPUT1_TERM;
import static com.sdu.calcite.plan.codegen.SduCodeGenUtils.DEFAULT_OUTPUT1_TERM;

import com.sdu.calcite.plan.SduCalciteTypeFactory;
import com.sdu.calcite.plan.codegen.SduCodeGeneratorContext;
import com.sdu.calcite.plan.exec.AbstractSduFunction;
import com.sdu.calcite.plan.exec.DataTransformation;
import com.sdu.calcite.plan.nodes.SduCommonCalc;
import com.sdu.calcite.plan.nodes.SduExecuteRel;
import com.sdu.calcite.plan.nodes.SduLogicalRel;
import com.sdu.calcite.table.types.SduLogicalType;
import com.sdu.calcite.table.types.SduRowType;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

public class SduLogicalCalc extends Calc implements SduLogicalRel, SduCommonCalc, SduExecuteRel {

  public SduLogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexProgram program) {
    super(cluster, traits, child, program);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
    return new SduLogicalCalc(getCluster(),
        traitSet,
        child,
        program);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double rowCnt = mq.getRowCount(this.getInput());
    return computeSelfCost(program, planner, rowCnt);
  }

  @Override
  public DataTransformation translateToPlanInternal() {
    SduExecuteRel input = (SduExecuteRel) getInput();
    DataTransformation inputTransformation = input.translateToPlanInternal();

    // Code Generate
    SduCodeGeneratorContext ctx = new SduCodeGeneratorContext()
        .setFunctionBaseClass(AbstractSduFunction.class.getName());

    // 输出数据类型
    SduLogicalType resultType = SduCalciteTypeFactory.toLogicalType(getRowType());

    // Project, Condition
    Optional<RexNode> condition = program.getCondition() == null ? Optional.empty()
                                                                 : Optional.of(program.expandLocalRef(program.getCondition()));
    return generateCalcTransformation(
        ctx,
        inputTransformation,
        program,
        condition,
        DEFAULT_INPUT1_TERM,
        (SduRowType) resultType,
        DEFAULT_OUTPUT1_TERM
    );
  }
}
