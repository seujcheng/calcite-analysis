package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduIntType extends SduLogicalType {

  private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
      Integer.class.getName());

  private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
      Integer.class.getName(),
      int.class.getName());

  public SduIntType() {
    super(true, SduLogicalTypeRoot.INTEGER);
  }

  public SduIntType(boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.INTEGER);
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
    return SduIntType.class.getName();
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
