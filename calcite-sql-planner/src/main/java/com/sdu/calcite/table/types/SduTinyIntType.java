package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduTinyIntType extends SduLogicalType {


  private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
      Byte.class.getName());

  private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
      Byte.class.getName(),
      byte.class.getName());

  public SduTinyIntType() {
    super(true, SduLogicalTypeRoot.TINYINT);
  }

  public SduTinyIntType(boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.TINYINT);
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
    return SduTinyIntType.class.getName();
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
