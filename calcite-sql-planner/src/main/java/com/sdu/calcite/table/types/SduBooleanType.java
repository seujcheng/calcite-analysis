package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduBooleanType extends SduLogicalType {

  private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
      Boolean.class.getName()
  );

  private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
      Boolean.class.getName(),
      boolean.class.getName()
  );

  public SduBooleanType() {
    super(true, SduLogicalTypeRoot.BOOLEAN);
  }

  public SduBooleanType(boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.BOOLEAN);
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
    return SduBooleanType.class.getName();
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }

}
