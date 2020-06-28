package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduSmallIntType extends SduLogicalType {

  private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
      Short.class.getName());

  private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
      Short.class.getName(),
      short.class.getName());

  public SduSmallIntType() {
    super(true, SduLogicalTypeRoot.SMALLINT);
  }

  public SduSmallIntType(boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.SMALLINT);
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
    return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz);
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }

}
