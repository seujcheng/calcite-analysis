package com.sdu.calcite.plan.codegen;

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

public class CodeGeneratorContext {


  // 取值表达式: (inputTerm, index) -> expr
  private final Map<Pair<String, Integer>, GeneratedExpression> reusableInputUnboxingExpressions = new HashMap<>();

  // 局部变量
  private String currentMethodNameForLocalVariables = "DEFAULT";
  private final Map<String, LinkedHashSet<String>> reusableLocalVariableStatements = new HashMap<>();

  public boolean nullCheck() {
    throw new RuntimeException();
  }

  public Optional<GeneratedExpression> getReusableInputUnboxingExpression(String inputTerm, int index) {
    Pair<String, Integer> key = Pair.of(inputTerm, index);
    return Optional.ofNullable(reusableInputUnboxingExpressions.get(key));
  }

  public void addReusableInputUnboxingExpression(String inputTerm, int index, GeneratedExpression expr) {
    Pair<String, Integer> key = Pair.of(inputTerm, index);
    reusableInputUnboxingExpressions.put(key, expr);
  }


  public String[] addReusableLocalVariables(Pair<String, String> ... fieldTypeAndName) {
    String[] fieldNames = Arrays.stream(fieldTypeAndName)
        .map(Pair::getRight)
        .map(CodeGenUtils::newName)
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

}
