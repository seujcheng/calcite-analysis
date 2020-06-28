package com.sdu.calcite.table.types;

import com.sdu.calcite.api.SduValidationException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SduVarCharType extends SduLogicalType {

  private static final int EMPTY_LITERAL_LENGTH = 0;
  private static final int MIN_LENGTH = 1;
  private static final int MAX_LENGTH = Integer.MAX_VALUE;

  private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
      String.class.getName(),
      byte[].class.getName());

  private final int length;

  public SduVarCharType(int length) {
    super(true, SduLogicalTypeRoot.VARCHAR);
    this.length = length;
  }

  public SduVarCharType(boolean isNullable, int length) {
    super(isNullable, SduLogicalTypeRoot.VARCHAR);
    if (length < MIN_LENGTH) {
      throw new SduValidationException(
          String.format(
              "Variable character string length must be between %d and %d (both inclusive).",
              MIN_LENGTH,
              MAX_LENGTH));
    }
    this.length = length;
  }

  private SduVarCharType(int length, boolean isNullable) {
    super(isNullable, SduLogicalTypeRoot.VARCHAR);
    this.length = length;
  }

  public static SduVarCharType ofEmptyLiteral() {
    return new SduVarCharType(EMPTY_LITERAL_LENGTH, false);
  }

  public int getLength() {
    return length;
  }

  @Override
  public List<SduLogicalType> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean supportsInputConversion(Class<?> clazz) {
    return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public boolean supportsOutputConversion(Class<?> clazz) {
    return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
