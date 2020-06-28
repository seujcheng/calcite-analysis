package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduDoubleType extends SduLogicalType {

  private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
      Double.class.getName());

  private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
      Double.class.getName(),
      double.class.getName());

  public SduDoubleType() {
    super(true, SduLogicalTypeRoot.DOUBLE);
  }

  public SduDoubleType(boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.DOUBLE);
  }

  @Override
  public List<SduLogicalType> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean supportsInputConversion(Class<?> clazz) {
    return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public boolean supportsOutputConversion(Class<?> clazz) {
    if (isNullable()) {
      return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }
    return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public String asSummaryString() {
    return SduDoubleType.class.getName();
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
