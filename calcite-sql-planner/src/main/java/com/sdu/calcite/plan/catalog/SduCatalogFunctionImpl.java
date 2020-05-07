package com.sdu.calcite.plan.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SduCatalogFunctionImpl implements SduCatalogFunction {

  private final Map<String, String> functionProps;

  public SduCatalogFunctionImpl(Map<String, String> functionProps) {
    this.functionProps = Objects.requireNonNull(functionProps);
  }


  @Override
  public Map<String, String> getProperties() {
    return functionProps;
  }

  @Override
  public boolean isTemporary() {
    // TODO:
    return true;
  }

  @Override
  public SduCatalogFunction copy() {
    return new SduCatalogFunctionImpl(new HashMap<>(functionProps));
  }
}
