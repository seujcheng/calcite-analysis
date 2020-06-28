package com.sdu.calcite.table.types;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum SduLogicalTypeRoot {

  CHAR(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.CHARACTER_STRING
  ),

  VARCHAR(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.CHARACTER_STRING
  ),

  BOOLEAN(
      SduLogicalTypeFamily.PREDEFINED
  ),

  BINARY(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.BINARY_STRING),

  VARBINARY(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.BINARY_STRING),

  // TODO DECIMAL

  TINYINT(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.NUMERIC,
      SduLogicalTypeFamily.EXACT_NUMERIC),

  SMALLINT(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.NUMERIC,
      SduLogicalTypeFamily.EXACT_NUMERIC),

  INTEGER(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.NUMERIC,
      SduLogicalTypeFamily.EXACT_NUMERIC),

  BIGINT(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.NUMERIC,
      SduLogicalTypeFamily.EXACT_NUMERIC),

  FLOAT(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.NUMERIC,
      SduLogicalTypeFamily.APPROXIMATE_NUMERIC),

  DOUBLE(
      SduLogicalTypeFamily.PREDEFINED,
      SduLogicalTypeFamily.NUMERIC,
      SduLogicalTypeFamily.APPROXIMATE_NUMERIC),

  // TODO Time, Date, Collection

  ROW(SduLogicalTypeFamily.CONSTRUCTED);

  private final Set<SduLogicalTypeFamily> families;

  SduLogicalTypeRoot(SduLogicalTypeFamily firstFamily, SduLogicalTypeFamily... otherFamilies) {
    this.families = Collections.unmodifiableSet(EnumSet.of(firstFamily, otherFamilies));
  }

  public Set<SduLogicalTypeFamily> getFamilies() {
    return families;
  }
}
