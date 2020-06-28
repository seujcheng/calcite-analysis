package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduFloatType extends SduLogicalType {

  private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
      Float.class.getName());

  private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
      Float.class.getName(),
      float.class.getName());

  public SduFloatType() {
    super(true, SduLogicalTypeRoot.FLOAT);
  }

  public SduFloatType(boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.FLOAT);
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
      return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
    }
    return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public String asSummaryString() {
    return SduFloatType.class.getName();
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
