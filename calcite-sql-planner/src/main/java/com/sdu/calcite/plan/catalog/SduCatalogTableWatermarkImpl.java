package com.sdu.calcite.plan.catalog;

import java.util.Objects;

public class SduCatalogTableWatermarkImpl implements SduCatalogTableWatermark {

  private final String watermarkColumn;
  private final String watermarkExpression;
  private final String watermarkDataType;

  public SduCatalogTableWatermarkImpl(
      String watermarkColumn,
      String watermarkExpression,
      String watermarkDataType) {
    this.watermarkColumn = Objects.requireNonNull(watermarkColumn);
    this.watermarkExpression = Objects.requireNonNull(watermarkExpression);
    this.watermarkDataType = Objects.requireNonNull(watermarkDataType);
  }

  @Override
  public String getWatermarkColumn() {
    return watermarkColumn;
  }

  @Override
  public String getWatermarkExpression() {
    return watermarkExpression;
  }

  @Override
  public String getWatermarkDataType() {
    return watermarkDataType;
  }

}
