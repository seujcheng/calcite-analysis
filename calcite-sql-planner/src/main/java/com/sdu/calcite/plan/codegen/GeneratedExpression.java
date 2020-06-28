package com.sdu.calcite.plan.codegen;

import com.sdu.calcite.table.types.SduLogicalType;
import java.util.Optional;

public class GeneratedExpression {

  // result variable
  private final String resultTerm;
  // nullable variable
  private final String nullTerm;
  // execute code
  private final String code;
  private final SduLogicalType resultType;
  private final Optional<Object> literalValue;

  public GeneratedExpression(
      String resultTerm,
      String nullTerm,
      String code,
      SduLogicalType resultType) {
    this(resultTerm, nullTerm, code, resultType, Optional.empty());
  }

  public GeneratedExpression(
      String resultTerm,
      String nullTerm,
      String code,
      SduLogicalType resultType,
      Optional<Object> literalValue) {
    this.resultTerm = resultTerm;
    this.nullTerm = nullTerm;
    this.code = code;
    this.resultType = resultType;
    this.literalValue = literalValue;
  }

  public String getResultTerm() {
    return resultTerm;
  }

  public String getNullTerm() {
    return nullTerm;
  }

  public String getCode() {
    return code;
  }

  public SduLogicalType getResultType() {
    return resultType;
  }

  public Optional<Object> getLiteralValue() {
    return literalValue;
  }

}
