package com.sdu.calcite.util;

import static java.lang.String.format;

import com.sdu.calcite.catelog.SduCalciteScalarFunction;
import com.sdu.calcite.entry.SduAggregateFunction;
import com.sdu.calcite.entry.SduScalarFunction;
import com.sdu.calcite.entry.SduTableFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import com.sdu.calcite.types.SduTypeFactory;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

public class UserDefinedFunctionUtils {

  private UserDefinedFunctionUtils() {
  }

  public static SqlFunction convertTableSqlFunction(SduTypeFactory typeFactory, String name, SduTableFunction tableFunction) {
    throw new RuntimeException();
  }

  public static SqlFunction convertScalarSqlFunction(SduTypeFactory typeFactory, String name, SduScalarFunction scalarFunction) {
    return new SduCalciteScalarFunction(typeFactory, name, scalarFunction);
  }

  public static SqlFunction convertAggregateFunction(SduTypeFactory typeFactory, String name, SduAggregateFunction aggFunction) {
    throw new RuntimeException();
  }

  public static Method getEvalMethodSignature(UserDefinedFunction definedFunction, Class<?>[] signatures) {
    return checkAndExtractMethods(definedFunction, "eval", signatures);
  }

  private static Method checkAndExtractMethods(UserDefinedFunction definedFunction, String methodName, Class<?>[] signatures) {
    try {
      Method method = definedFunction.getClass().getMethod(methodName, signatures);
      int modifiers = method.getModifiers();
      if (Modifier.isPublic(modifiers) && !Modifier.isAbstract(modifiers)) {
        return method;
      }

      throw new RuntimeException(format("Function class %s does not implement at least one method named '%s' which is "
          + "public, not abstract and (in case of table functions) not static.", definedFunction.getClass().getName(), methodName));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(format("Function class %s does not implement at least one method named %s(%s).",
          definedFunction.getClass().getName(), methodName, formatSignature(signatures)));
    }
  }

  public static SqlOperandTypeInference createEvalOperandTypeInference(SduTypeFactory typeFactory, SduScalarFunction function) {
    throw new RuntimeException();
  }

  public static SqlOperandTypeChecker createEvalOperandTypeChecker(SduTypeFactory typeFactory, SduScalarFunction function) {
    throw new RuntimeException();
  }

  private static String formatSignature(Class<?>[] signatures) {
    if (signatures == null || signatures.length == 0) return "";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < signatures.length; ++i) {
      if (i == 0) {
        sb.append(signatures[i].getSimpleName());
      } else {
        sb.append(", ").append(signatures[i].getSimpleName());
      }
    }
    return sb.toString();
  }
}
