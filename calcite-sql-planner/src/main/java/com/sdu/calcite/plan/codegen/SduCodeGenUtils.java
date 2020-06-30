package com.sdu.calcite.plan.codegen;

import static java.lang.String.format;
import static java.lang.String.join;

import com.google.common.base.Preconditions;
import com.sdu.calcite.table.data.SduGenericRowData;
import com.sdu.calcite.table.data.SduRowData;
import com.sdu.calcite.table.types.SduLogicalType;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;

public class SduCodeGenUtils {

  private static final AtomicLong nameCounter = new AtomicLong(0);

  public static final String DEFAULT_INPUT1_TERM = "in1";
  static final String DEFAULT_INPUT2_TERM = "in2";

  public static final String DEFAULT_OUTPUT1_TERM = "out1";

  private SduCodeGenUtils() {

  }

  public static String newName(String name) {
    return String.format("%s$$%d", name, nameCounter.getAndIncrement());
  }


  public static SduGeneratedExpression generateInputAccess(
      SduCodeGeneratorContext ctx,
      SduLogicalType inputType,
      String inputTerm,
      int index,
      boolean nullableInput) {
    SduGeneratedExpression inputExpr = ctx.getReusableInputUnboxingExpression(inputTerm, index)
        .orElseGet(() -> {
          if (nullableInput) {
            throw new UnsupportedOperationException("Unsupported");
          }

          SduGeneratedExpression expr = generateFieldAccess(ctx, inputType, inputTerm, index);
          ctx.addReusableInputUnboxingExpression(inputTerm, index, expr);

          return expr;
        });

    return new SduGeneratedExpression(
        inputExpr.getResultTerm(),
        inputExpr.getNullTerm(),
        "",
        inputExpr.getResultType(),
        inputExpr.getLiteralValue()
    );
  }

  public static String rowSetField(
      SduCodeGeneratorContext ctx,
      Class<? extends SduRowData> rowClass,
      String rowTerm,
      int index,
      SduGeneratedExpression fieldExpr) {
    if (rowClass == SduGenericRowData.class) {
      String writeField = format("%s.setField(%d, %s);", rowTerm, index, fieldExpr.getResultTerm());
      String setNullField = format("%s.setField(%d, null);", rowTerm, index);

      String setCode;
      if (ctx.nullCheck()) {
        String codeTemplate = "%s \n"
                            + "if (%s) { \n"
                            + "  %s \n"
                            + "} else { \n"
                            + "  %s \n"
                            + "}\n";
        setCode = format(codeTemplate,
            fieldExpr.getCode(),
            fieldExpr.getNullTerm(),
            setNullField,
            writeField);
      } else {
        // read, write
        setCode = join("\n", fieldExpr.getCode(), writeField);
      }

      return setCode;
    }

    throw new UnsupportedOperationException("Not support set field for " + rowClass);
  }


  private static SduGeneratedExpression generateFieldAccess(SduCodeGeneratorContext ctx, SduLogicalType inputType, String inputTerm, int index) {
    switch (inputType.getTypeRoot()) {
      case ROW:
        SduLogicalType fieldType = inputType.getChildren().get(index);
        String resultTypeTerm = primitiveTypeTermForType(fieldType);
        String defaultValue = primitiveDefaultValue(fieldType);
        String readCode = rowFieldReadAccess(ctx, inputTerm, index, fieldType);
        @SuppressWarnings("unchecked")
        String[] variables = ctx.addReusableLocalVariables(
            Pair.of(resultTypeTerm, "field"),
            Pair.of("boolean", "isNull")
        );
        Preconditions.checkArgument(variables.length == 2);
        String inputCode;
        if (ctx.nullCheck()) {
          String codeTemplate = "%s = %s.isNullAt(%d); \n"
                              + "%s = %s; \n"
                              + "if (!%s) { \n"
                              + "   %s = %s; \n"
                              + "} \n";
          inputCode = format(
              codeTemplate,
              variables[1], inputTerm, index,
              variables[0], defaultValue,
              variables[1],
              variables[0], readCode
          );
        } else {
          String codeTemplate = "%s = false; \n"
                              + "%s = %s; \n";
          inputCode = format(
              codeTemplate,
              variables[1],
              variables[0], readCode
          );
        }

        return new SduGeneratedExpression(variables[0], variables[1], inputCode, fieldType);

        default:
          throw new UnsupportedOperationException("wait develop");
    }
  }

  private static String rowFieldReadAccess(
      SduCodeGeneratorContext ctx,
      String inputTerm,
      int index,
      SduLogicalType fieldType) {
    switch (fieldType.getTypeRoot()) {
      case CHAR :
      case VARCHAR:
        return format("%s.getString(%d)", inputTerm, index);

      case BOOLEAN:
        return format("%s.getBoolean(%d)", inputTerm, index);

      case BINARY:
      case VARBINARY:
        return format("%s.getBinary(%d)", inputTerm, index);

      case TINYINT:
        return format("%s.getByte(%d)", inputTerm, index);

      case SMALLINT:
        return format("%s.getShort(%d)", inputTerm, index);

      case INTEGER:
        return format("%s.getInt(%d)", inputTerm, index);

      case BIGINT:
        return format("%s.getLong(%d)", inputTerm, index);

      case FLOAT:
        return format("%s.getFloat(%d)", inputTerm, index);

      case DOUBLE:
        return format("%s.getDouble(%d)", inputTerm, index);

      default:
        throw new IllegalArgumentException("Illegal type: " + fieldType);
    }
  }

  private static String primitiveTypeTermForType(SduLogicalType t) {
    switch (t.getTypeRoot()) {
      case BOOLEAN: return "boolean";

      case TINYINT: return "byte";

      case SMALLINT: return "short";

      case INTEGER: return "int";

      case BIGINT: return "long";

      case FLOAT: return "float";

      case DOUBLE: return "double";

      default: return boxedTypeTermForType(t);
    }
  }

  private static String boxedTypeTermForType(SduLogicalType t) {
    switch (t.getTypeRoot()) {
      case CHAR:
      case VARCHAR: return String.class.getCanonicalName();

      case BINARY:
      case VARBINARY: return byte[].class.getCanonicalName();

      default:
        throw new IllegalArgumentException("Illegal type: " + t);
    }
  }

  private static String primitiveDefaultValue(SduLogicalType t) {
    switch (t.getTypeRoot()) {
      case CHAR:
      case VARCHAR: return "\"\"";

      case BOOLEAN: return "false";

      case TINYINT:
      case SMALLINT:
      case INTEGER: return "-1";

      case BIGINT: return "-1L";

      case FLOAT: return "-1.0f";

      case DOUBLE: return "-1.0d";

      default: return null;
    }
  }

}
