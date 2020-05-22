package com.sdu.calcite.plan.catalog;

public interface SduCatalogTableWatermark {

  String getWatermarkColumn();

  String getWatermarkExpression();

  String getWatermarkDataType();

}
