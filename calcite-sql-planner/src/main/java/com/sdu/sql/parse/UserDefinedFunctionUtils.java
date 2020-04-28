package com.sdu.sql.parse;

import static java.lang.String.format;

import com.sdu.calcite.plan.SduCalciteTypeFactory;
import com.sdu.calcite.catelog.SduCalciteAggFunction;
import com.sdu.calcite.catelog.SduCalciteScalarFunction;
import com.sdu.calcite.catelog.SduCalciteTableFunction;
import com.sdu.calcite.catelog.SduCalciteTableFunctionImpl;
import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.TableFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import com.sdu.sql.entry.SduAggFunction;
import com.sdu.sql.entry.SduFunction;
import com.sdu.sql.entry.SduScalarFunction;
import com.sdu.sql.entry.SduTableFunction;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

public class UserDefinedFunctionUtils {

  private UserDefinedFunctionUtils() {
  }

  public static SqlFunction convertTableSqlFunction(SduCalciteTypeFactory typeFactory, String name, SduTableFunction tableFunction) {
    TableFunction<?> function = tableFunction.getTableFunction();
    SduCalciteTableFunctionImpl functionImpl = new SduCalciteTableFunctionImpl(function.getColumnNames(), function.getColumnTypes());
    return new SduCalciteTableFunction(name, typeFactory, tableFunction, functionImpl);
  }

  public static SqlFunction convertScalarSqlFunction(SduCalciteTypeFactory typeFactory, String name, SduScalarFunction scalarFunction) {
    return new SduCalciteScalarFunction(typeFactory, name, scalarFunction);
  }

  public static SqlFunction convertAggregateFunction(SduCalciteTypeFactory typeFactory, String name, SduAggFunction aggFunction) {
    return new SduCalciteAggFunction(name, typeFactory, aggFunction);
  }

  public static Method getEvalMethod(UserDefinedFunction definedFunction, Class<?>[] signatures) {
    return getEvalMethod(definedFunction, signatures, true);
  }

  public static Method getEvalMethod(UserDefinedFunction definedFunction, Class<?>[] signatures, boolean throwOnFailure) {
    return checkAndExtractMethod(definedFunction, "eval", signatures, throwOnFailure);
  }

  public static Method getAccumulateMethod(UserDefinedFunction definedFunction, Class<?>[] signatures) {
    return getEvalMethod(definedFunction, signatures, true);
  }

  public static Method getAccumulateMethod(UserDefinedFunction definedFunction, Class<?>[] signatures, boolean throwOnFailure) {
    return checkAndExtractMethod(definedFunction, "accumulate", signatures, throwOnFailure);
  }

  private static Method checkAndExtractMethod(UserDefinedFunction definedFunction, String methodName, Class<?>[] signatures, boolean throwOnFailure) {
    try {
      Method method = definedFunction.getClass().getMethod(methodName, signatures);
      int modifiers = method.getModifiers();
      if (Modifier.isPublic(modifiers) && !Modifier.isAbstract(modifiers)) {
        return method;
      }
      if (throwOnFailure) {
        throw new RuntimeException(format("Function class %s does not implement at least one method named '%s' which is "
            + "public, not abstract and (in case of table functions) not static.", definedFunction.getClass().getName(), methodName));
      }
      return null;
    } catch (NoSuchMethodException e) {
      if (throwOnFailure) {
        throw new RuntimeException(format("Function class %s does not implement at least one method named %s(%s).",
            definedFunction.getClass().getName(), methodName, signaturesToString(signatures)));
      }
      return null;
    }
  }

  private static Method[] checkAndExtractMethods(UserDefinedFunction definedFunction, String methodName) {
    Method[] methods = Arrays.stream(definedFunction.getClass().getMethods())
        .filter(method -> {
          int modifiers = method.getModifiers();
          return method.getName().equals(methodName)
              && Modifier.isPublic(modifiers)
              && !Modifier.isAbstract(modifiers);
        })
        .toArray(Method[]::new);

    if (methods.length == 0) {
      throw new RuntimeException(format("Function class %s does not implement at least one method named '%s' which is "
          + "public, not abstract and (in case of table functions) not static.", definedFunction.getClass().getName(), methodName));
    }

    return methods;
  }

  public static SqlOperandTypeInference createEvalOperandTypeInference(
      SduCalciteTypeFactory typeFactory, SduFunction function) {

    class SqlOperandTypeInferenceImpl implements SqlOperandTypeInference {

      private final SduCalciteTypeFactory typeFactory;
      private final SduFunction function;

      private SqlOperandTypeInferenceImpl(SduCalciteTypeFactory typeFactory, SduFunction function) {
        this.typeFactory = typeFactory;
        this.function = function;
      }

      @Override
      public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        if (function.getKind() != FunctionKind.SCALAR && function.getKind() != FunctionKind.TABLE) {
          throw new RuntimeException("Unsupported function, kind: " + function.getKind());
        }

        Class<?>[] operandTypeInfo = getOperandTypeInfo(callBinding, typeFactory);
        Method method = getEvalMethod(function.getUserDefinedFunction(), operandTypeInfo);
        Class<?>[] signatures = method.getParameterTypes();
        for (int i = 0; i < signatures.length; ++i) {
          operandTypes[i] = typeFactory.createSqlType(signatures[i].getName());
        }
      }
    }

    return new SqlOperandTypeInferenceImpl(typeFactory, function);
  }

  public static SqlOperandTypeChecker createEvalOperandTypeChecker(SduCalciteTypeFactory typeFactory, SduFunction function) {

    class SqlOperandTypeCheckerImpl implements SqlOperandTypeChecker {

      private final SduCalciteTypeFactory typeFactory;
      private final SduFunction function;

      private final Method[] methods;

      private SqlOperandTypeCheckerImpl(SduCalciteTypeFactory typeFactory, SduFunction function) {
        this.typeFactory = typeFactory;
        this.function = function;
        this.methods = checkAndExtractMethods(function.getUserDefinedFunction(), "eval");
      }

      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        Class<?>[] operandTypeInfo = getOperandTypeInfo(callBinding, typeFactory);
        Method method = getEvalMethod(function.getUserDefinedFunction(), operandTypeInfo, false);
        if (method == null) {
          if (throwOnFailure) {
            throw new RuntimeException(format("Function class %s does not implement at least one method named eval(%s).",
                function.getUserDefinedFunction().getClass().getName(), signaturesToString(operandTypeInfo)));
          }
          return false;
        }
        return true;
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        int min = 254;
        int max = -1;
        boolean isVarargs = false;
        for (Method m : methods) {
          int len = m.getParameterTypes().length;
          if (len > 0 && m.isVarArgs() && m.getParameterTypes()[len - 1].isArray()) {
            isVarargs = true;
            len = len - 1;
          }
          max = Math.max(len, max);
          min = Math.min(len, min);
        }
        if (isVarargs) {
          // if eval method is varargs, set max to -1 to skip length check in Calcite
          max = -1;
        }
        return SqlOperandCountRanges.between(min, max);
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return format("opName[%s]", opName);
      }

      @Override
      public Consistency getConsistency() {
        return Consistency.NONE;
      }

      @Override
      public boolean isOptional(int i) {
        return false;
      }
    }

    return new SqlOperandTypeCheckerImpl(typeFactory, function);
  }

  public static Class<?>[] getOperandTypeInfo(SqlCallBinding callBinding, SduCalciteTypeFactory typeFactory) {
    return IntStream.range(0, callBinding.getOperandCount())
        .mapToObj(i -> typeFactory.getJavaClass(callBinding.getOperandType(i)))
        .toArray(Class<?>[]::new);
  }

  private static String signaturesToString(Class<?>[] signatures) {
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
