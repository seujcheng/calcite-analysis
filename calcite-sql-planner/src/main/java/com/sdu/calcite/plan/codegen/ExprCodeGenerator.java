package com.sdu.calcite.plan.codegen;

import static com.sdu.calcite.plan.codegen.CodeGenUtils.DEFAULT_INPUT1_TERM;
import static com.sdu.calcite.plan.codegen.CodeGenUtils.DEFAULT_INPUT2_TERM;
import static com.sdu.calcite.plan.codegen.CodeGenUtils.generateInputAccess;

import com.sdu.calcite.plan.SduCalciteTypeFactory;
import com.sdu.calcite.table.types.SduLogicalType;
import com.sdu.calcite.table.types.SduRowType;
import java.util.Optional;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

public class ExprCodeGenerator implements RexVisitor<GeneratedExpression> {

  private final CodeGeneratorContext ctx;
  private final boolean nullableInput;

  // 输入信息: JOIN算子需要两个输入数据, 其他算子需要一个输入数据
  private SduLogicalType inputType1;
  private String inputTerm1;
  private Optional<int[]> inputFieldMapping1;

  private Optional<SduLogicalType> inputType2;
  private Optional<String> inputTerm2;
  private Optional<int[]> inputFieldMapping2;

  public ExprCodeGenerator(CodeGeneratorContext ctx, boolean nullableInput) {
    this.ctx = ctx;
    this.nullableInput = nullableInput;
  }

  public void bindInput(SduLogicalType inputType, int[] inputFieldMapping) {
    this.inputType1 = inputType;
    this.inputTerm1 = DEFAULT_INPUT1_TERM;
    this.inputFieldMapping1 = Optional.ofNullable(inputFieldMapping);
  }

  public void bindSecondInput(SduLogicalType inputType, int[] inputFieldMapping) {
    this.inputType2 = Optional.of(inputType);
    this.inputTerm2 = Optional.of(DEFAULT_INPUT2_TERM);
    this.inputFieldMapping2 = Optional.ofNullable(inputFieldMapping);
  }

  public GeneratedExpression generateExpression(RexNode node) {
    return node.accept(this);
  }

  @Override
  public GeneratedExpression visitInputRef(RexInputRef inputRef) {
    int input1Arity = inputType1 instanceof SduRowType ? ((SduRowType) inputType1).getFieldCount() : 1;
    int index = inputRef.getIndex();

    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    if (index < input1Arity) {
      return generateInputAccess(ctx, inputType1, inputTerm1, index, nullableInput);
    }

    return generateInputAccess(
        ctx,
        inputType2.orElseThrow(() -> new CodeGenException("Invalid input access.")),
        inputTerm2.orElseThrow(() -> new CodeGenException("Invalid input access.")),
        index,
        nullableInput
    );
  }

  @Override
  public GeneratedExpression visitLocalRef(RexLocalRef localRef) {
    throw new CodeGenException("RexLocalRef are not supported yet.");
  }

  @Override
  public GeneratedExpression visitLiteral(RexLiteral literal) {
    SduLogicalType resultType = SduCalciteTypeFactory.toLogicalType(literal.getType());

    return null;
  }

  @Override
  public GeneratedExpression visitCall(RexCall call) {
    // TODO:
    return null;
  }

  @Override
  public GeneratedExpression visitOver(RexOver over) {
    throw new CodeGenException("Aggregate functions over windows are not supported yet.");
  }

  @Override
  public GeneratedExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
    // TODO:
    return null;
  }

  @Override
  public GeneratedExpression visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new CodeGenException("Dynamic parameter references are not supported yet.");
  }

  @Override
  public GeneratedExpression visitRangeRef(RexRangeRef rangeRef) {
    throw new CodeGenException("Range references are not supported yet.");
  }

  @Override
  public GeneratedExpression visitFieldAccess(RexFieldAccess fieldAccess) {
    // TODO:
    return null;
  }

  @Override
  public GeneratedExpression visitSubQuery(RexSubQuery subQuery) {
    throw new CodeGenException("SubQuery are not supported yet.");
  }

  @Override
  public GeneratedExpression visitTableInputRef(RexTableInputRef fieldRef) {
    // TODO:
    return null;
  }

  @Override
  public GeneratedExpression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new CodeGenException("Pattern field references are not supported yet.");
  }

}
