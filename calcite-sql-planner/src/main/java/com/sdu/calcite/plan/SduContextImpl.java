package com.sdu.calcite.plan;

import com.sdu.calcite.api.SduTableConfig;

public class SduContextImpl implements SduContext {

  private final SduTableConfig tableConfig;
  private final SduSqlExprToRexConverterFactory teRexFactory;

  SduContextImpl(
      SduTableConfig tableConfig,
      SduSqlExprToRexConverterFactory teRexFactory) {
    this.tableConfig = tableConfig;
    this.teRexFactory = teRexFactory;
  }

  @Override
  public SduTableConfig getTableConfig() {
    return tableConfig;
  }

  @Override
  public SduSqlExprToRexConverterFactory getSqlExprToRexConverterFactory() {
    return teRexFactory;
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return null;
  }

}
