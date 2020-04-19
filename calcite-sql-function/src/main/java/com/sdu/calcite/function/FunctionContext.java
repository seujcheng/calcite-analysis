package com.sdu.calcite.function;

public interface FunctionContext {

  <C> C unwrap(Class<C> cls);

}
