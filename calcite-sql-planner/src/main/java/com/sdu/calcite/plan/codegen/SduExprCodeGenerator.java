package com.sdu.calcite.plan.codegen;

import static com.sdu.calcite.plan.codegen.SduCodeGenUtils.DEFAULT_INPUT2_TERM;
import static com.sdu.calcite.plan.codegen.SduCodeGenUtils.generateInputAccess;
import static com.sdu.calcite.plan.codegen.SduCodeGenUtils.rowSetField;
import static java.lang.String.format;

import com.sdu.calcite.plan.SduCalciteTypeFactory;
import com.sdu.calcite.table.data.SduGenericRowData;
import com.sdu.calcite.table.data.SduRowData;
import com.sdu.calcite.table.types.SduLogicalType;
import com.sdu.calcite.table.types.SduRowType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

public class SduExprCodeGenerator implements RexVisitor<SduGeneratedExpression> {

  private final SduCodeGeneratorContext ctx;
  private final boolean nullableInput;

  // 输入信息: JOIN算子需要两个输入数据, 其他算子需要一个输入数据
  private SduLogicalType inputType1;
  private String inputTerm1;
  private Optional<int[]> inputFieldMapping1;

  private Optional<SduLogicalType> inputType2;
  private Optional<String> inputTerm2;
  private Optional<int[]> inputFieldMapping2;

  public SduExprCodeGenerator(SduCodeGeneratorContext ctx, boolean nullableInput) {
    this.ctx = ctx;
    this.nullableInput = nullableInput;
  }

  public SduExprCodeGenerator bindInput(SduLogicalType inputType, String inputTerm, int[] inputFieldMapping) {
    this.inputType1 = inputType;
    this.inputTerm1 = inputTerm;
    this.inputFieldMapping1 = Optional.ofNullable(inputFieldMapping);
    return this;
  }

  public SduExprCodeGenerator bindSecondInput(SduLogicalType inputType, int[] inputFieldMapping) {
    this.inputType2 = Optional.of(inputType);
    this.inputTerm2 = Optional.of(DEFAULT_INPUT2_TERM);
    this.inputFieldMapping2 = Optional.ofNullable(inputFieldMapping);
    return this;
  }

  public SduGeneratedExpression generateExpression(RexNode node) {
    return node.accept(this);
  }

  public SduGeneratedExpression generateResultExpression(
      SduCodeGeneratorContext ctx,
      List<SduGeneratedExpression> fieldExpressions,
      SduRowType resultType,
      Class<? extends SduRowData> outClass,
      String outRowTerm) {
    // 校验
    if (resultType.getFieldCount() != fieldExpressions.size()) {
      throw new SduCodeGenException(format("Arity [%d] of result type [%s] does not match number [%d] of expressions [%s].",
          resultType.getFieldCount(), resultType, fieldExpressions.size(), fieldExpressions));
    }
    Iterator<SduGeneratedExpression> fieldTypes = fieldExpressions.iterator();
    Iterator<SduLogicalType> resultTypes = resultType.getChildren().iterator();
    while (fieldTypes.hasNext() && resultTypes.hasNext()) {
      SduGeneratedExpression expr = fieldTypes.next();
      SduLogicalType returnType = resultTypes.next();
      if (returnType.getTypeRoot() != expr.getResultType().getTypeRoot()) {
        throw new SduCodeGenException(format("Incompatible types of expression and result type. Expression[%s] type"
            + " is [%s], result type is [%s]", expr, expr.getResultType(), resultType));
      }
    }

    // setField
    List<String> setFieldCodes = new ArrayList<>();
    int i = 0;
    for (SduGeneratedExpression expr : fieldExpressions) {
      setFieldCodes.add(rowSetField(ctx, SduGenericRowData.class, outRowTerm, i++, expr));
    }
    StringBuilder setFieldCode = new StringBuilder();
    for (String code : setFieldCodes) {
      setFieldCode.append(code);
      setFieldCode.append("\n");
    }

    // Out row initialize
    String outRowInitCode = format("private final %s %s = new %s(%d);",
        outClass.getName(), outRowTerm, outClass.getName(), resultType.getFieldCount());
    ctx.addReusableMember(outRowInitCode);

    // Result code
    String returnCode = format("return %s;", outRowTerm);

    String bodyCode = format("%s \n %s", setFieldCode.toString(), returnCode);

    return new SduGeneratedExpression(outRowTerm, "false", bodyCode, resultType);
  }


  @Override
  public SduGeneratedExpression visitInputRef(RexInputRef inputRef) {
    int input1Arity = inputType1 instanceof SduRowType ? ((SduRowType) inputType1).getFieldCount() : 1;
    int index = inputRef.getIndex();

    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    if (index < input1Arity) {
      return generateInputAccess(ctx, inputType1, inputTerm1, index, nullableInput);
    }

    return generateInputAccess(
        ctx,
        inputType2.orElseThrow(() -> new SduCodeGenException("Invalid input access.")),
        inputTerm2.orElseThrow(() -> new SduCodeGenException("Invalid input access.")),
        index,
        nullableInput
    );
  }

  @Override
  public SduGeneratedExpression visitLocalRef(RexLocalRef localRef) {
    throw new SduCodeGenException("RexLocalRef are not supported yet.");
  }

  @Override
  public SduGeneratedExpression visitLiteral(RexLiteral literal) {
    SduLogicalType resultType = SduCalciteTypeFactory.toLogicalType(literal.getType());

    return null;
  }

  @Override
  public SduGeneratedExpression visitCall(RexCall call) {
    // TODO:
    return null;
  }

  @Override
  public SduGeneratedExpression visitOver(RexOver over) {
    throw new SduCodeGenException("Aggregate functions over windows are not supported yet.");
  }

  @Override
  public SduGeneratedExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
    // TODO:
    return null;
  }

  @Override
  public SduGeneratedExpression visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new SduCodeGenException("Dynamic parameter references are not supported yet.");
  }

  @Override
  public SduGeneratedExpression visitRangeRef(RexRangeRef rangeRef) {
    throw new SduCodeGenException("Range references are not supported yet.");
  }

  @Override
  public SduGeneratedExpression visitFieldAccess(RexFieldAccess fieldAccess) {
    // TODO:
    return null;
  }

  @Override
  public SduGeneratedExpression visitSubQuery(RexSubQuery subQuery) {
    throw new SduCodeGenException("SubQuery are not supported yet.");
  }

  @Override
  public SduGeneratedExpression visitTableInputRef(RexTableInputRef fieldRef) {
    // TODO:
    return null;
  }

  @Override
  public SduGeneratedExpression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new SduCodeGenException("Pattern field references are not supported yet.");
  }

}
