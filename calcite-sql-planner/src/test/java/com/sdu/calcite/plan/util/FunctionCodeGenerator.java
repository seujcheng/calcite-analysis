package com.sdu.calcite.plan.util;

import static java.lang.String.format;

import com.sdu.calcite.plan.codegen.SduCodeGenException;
import com.sdu.calcite.plan.codegen.SduCodeGenUtils;
import com.sdu.calcite.plan.codegen.SduCodeGeneratorContext;
import com.sdu.calcite.plan.exec.SduFunction;
import com.sdu.calcite.table.data.SduRowData;

public class FunctionCodeGenerator {

  private FunctionCodeGenerator() {

  }

  public static SduFunction generateFunction(
      String name,
      SduCodeGeneratorContext ctx,
      String inputTerm,
      String bodyCode,
      Class<SduRowData> inClass,
      Class<SduRowData> outClass) {

    String funcName = SduCodeGenUtils.newName(name);

    String functionTemplate = "public class %s extends %s { \n"
                            + "  // init member \n"
                            + "   %s \n"
                            + "   public %s process(%s %s) { \n"
                            + "      // init local variable \n"
                            + "      %s \n"
                            + "      // read row data \n"
                            + "      %s \n"
                            + "      %s \n"
                            + "   }\n"
                            + "}";

    String code = format(functionTemplate,
        funcName, ctx.getFunctionBaseClass(),
        ctx.reuseMemberCode(),
        outClass.getName(), inClass.getName(), inputTerm,
        ctx.reuseLocalVariableCode(null),
        ctx.reuseInputUnboxingExpression(),
        bodyCode
    );

    try {
      return (SduFunction) SduCompileUtils.compile(
          Thread.currentThread().getContextClassLoader(),
          funcName,
          code
      ).newInstance();
    } catch (Exception e) {
      throw new SduCodeGenException(e);
    }


  }

}
