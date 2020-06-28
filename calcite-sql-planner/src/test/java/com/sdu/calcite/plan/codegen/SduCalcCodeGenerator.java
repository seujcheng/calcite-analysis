package com.sdu.calcite.plan.codegen;

import static com.sdu.calcite.plan.util.FunctionCodeGenerator.generateFunction;

import com.sdu.calcite.plan.exec.DataTransformation;
import com.sdu.calcite.plan.exec.GeneralDataTransformation;
import com.sdu.calcite.plan.exec.SduFunction;
import com.sdu.calcite.table.data.SduGenericRowData;
import com.sdu.calcite.table.data.SduRowData;
import com.sdu.calcite.table.types.SduRowType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

public class SduCalcCodeGenerator {

  private SduCalcCodeGenerator() {

  }

  public static DataTransformation generateCalcTransformation(
      SduCodeGeneratorContext ctx,
      DataTransformation inputTransformation,
      RexProgram program,
      Optional<RexNode> condition,
      String inputTerm,
      SduRowType outType,
      String outTerm) {
    // step1: SduFunction
    String functionCode = generateProcessCode(ctx,
        inputTransformation.getOutputType(),
        inputTerm,
        outType,
        SduGenericRowData.class,
        outTerm,
        program,
        condition);

    SduFunction function = generateFunction(
        "DataExecCalc",
        ctx,
        inputTerm,
        functionCode,
        SduRowData.class,
        SduRowData.class);
    // step2: DataTransformation
    return new GeneralDataTransformation(inputTransformation, function, outType);
  }

  private static String generateProcessCode(
      SduCodeGeneratorContext ctx,
      SduRowType inputType,
      String inputTerm,
      SduRowType outType,
      Class<? extends SduRowData> outClass,
      String outRowTerm,
      RexProgram calcProgram,
      Optional<RexNode> condition) {
    SduExprCodeGenerator codeGenerator = new SduExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm, null);

    if (condition.isPresent()) {
      throw new UnsupportedOperationException("unsupported");
    }

    List<RexNode> projects = calcProgram.getProjectList()
        .stream()
        .map(calcProgram::expandLocalRef)
        .collect(Collectors.toList());
    return produceProjectionCode(ctx, codeGenerator, projects, outType, outClass, outRowTerm);
  }


  private static String produceProjectionCode(
      SduCodeGeneratorContext ctx,
      SduExprCodeGenerator codeGenerator,
      List<RexNode> projects,
      SduRowType outType,
      Class<? extends SduRowData> outClass,
      String outRowTerm) {
    List<SduGeneratedExpression> expr = projects.stream()
        .map(codeGenerator::generateExpression)
        .collect(Collectors.toList());

    return codeGenerator.generateResultExpression(ctx, expr, outType, outClass, outRowTerm).getCode();
  }

}
