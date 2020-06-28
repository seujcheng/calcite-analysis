package com.sdu.calcite.plan.codegen;

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

public class SduCodeGeneratorContext {

  private String functionBaseClass;

  // 成员变量
  private final Set<String> reusableMemberStatements = new LinkedHashSet<>();

  // 取值表达式: (inputTerm, index) -> expr
  private final Map<Pair<String, Integer>, SduGeneratedExpression> reusableInputUnboxingExpressions = new HashMap<>();

  // 局部变量
  private String currentMethodNameForLocalVariables = "DEFAULT";
  private final Map<String, LinkedHashSet<String>> reusableLocalVariableStatements = new HashMap<>();

  public boolean nullCheck() {
    // TODO: 2020-06-28
    return true;
  }

  public SduCodeGeneratorContext setFunctionBaseClass(String className) {
    this.functionBaseClass = className;
    return this;
  }

  public String getFunctionBaseClass() {
    return this.functionBaseClass;
  }

  public void addReusableMember(String memberStatement) {
    reusableMemberStatements.add(memberStatement);
  }

  public String reuseMemberCode() {
    StringBuilder sb = new StringBuilder();
    for (String memberStatement : reusableMemberStatements) {
      sb.append(memberStatement);
      sb.append("\n");
    }
    return sb.toString();
  }

  public Optional<SduGeneratedExpression> getReusableInputUnboxingExpression(String inputTerm, int index) {
    Pair<String, Integer> key = Pair.of(inputTerm, index);
    return Optional.ofNullable(reusableInputUnboxingExpressions.get(key));
  }

  public void addReusableInputUnboxingExpression(String inputTerm, int index, SduGeneratedExpression expr) {
    Pair<String, Integer> key = Pair.of(inputTerm, index);
    reusableInputUnboxingExpressions.put(key, expr);
  }

  public String reuseInputUnboxingExpression() {
    StringBuilder sb = new StringBuilder();
    for (SduGeneratedExpression expr : reusableInputUnboxingExpressions.values()) {
      sb.append(expr.getCode());
      sb.append("\n");
    }
    return sb.toString();
  }


  public String[] addReusableLocalVariables(Pair<String, String> ... fieldTypeAndName) {
    String[] fieldNames = Arrays.stream(fieldTypeAndName)
        .map(Pair::getRight)
        .map(SduCodeGenUtils::newName)
        .toArray(String[]::new);

    String[] fieldTypes = Arrays.stream(fieldTypeAndName)
        .map(Pair::getLeft)
        .toArray(String[]::new);

    Preconditions.checkArgument(fieldNames.length == fieldTypes.length);
    for (int i = 0; i < fieldNames.length; ++i) {
      LinkedHashSet<String> methodLocalVariable = reusableLocalVariableStatements.computeIfAbsent(currentMethodNameForLocalVariables, key -> new LinkedHashSet<>());
      // NOTE: 只声明
      methodLocalVariable.add(format("%s %s;", fieldTypes[i], fieldNames[i]));
    }

    return fieldNames;
  }

  public String reuseLocalVariableCode(String methodName) {
    if (methodName == null) {
      methodName = currentMethodNameForLocalVariables;
    }
    Set<String> variableCodes = reusableLocalVariableStatements.get(methodName);
    if (variableCodes == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (String code : variableCodes) {
      sb.append(code);
      sb.append("\n");
    }
    return sb.toString();
  }

}
