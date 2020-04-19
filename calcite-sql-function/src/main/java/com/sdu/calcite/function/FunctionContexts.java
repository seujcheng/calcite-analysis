package com.sdu.calcite.function;

import static java.util.Objects.requireNonNull;

import java.util.LinkedList;
import java.util.List;

public class FunctionContexts {

  private FunctionContexts() {}

  public static FunctionContext of(Object param) {
    return new WrapFunctionContext(param);
  }

  public static FunctionContext of(Object ... params) {
    List<WrapFunctionContext> contexts = new LinkedList<>();
    for (Object param : params) {
      contexts.add(new WrapFunctionContext(param));
    }
    return new  ChainFunctionContext(contexts);
  }

  private static class WrapFunctionContext implements FunctionContext {

    final Object target;

    private WrapFunctionContext(Object target) {
      this.target = requireNonNull(target);
    }

    @Override
    public <C> C unwrap(Class<C> cls) {
      if (cls.isInstance(target)) {
        return cls.cast(target);
      }
      return null;
    }
  }

  private static class ChainFunctionContext implements FunctionContext {

    final List<? extends FunctionContext> contexts;

    private ChainFunctionContext(List<? extends FunctionContext> contexts) {
      this.contexts = requireNonNull(contexts);
      for (FunctionContext context : contexts) {
        assert !(context instanceof ChainFunctionContext) : "must be flat";
      }
    }

    @Override
    public <C> C unwrap(Class<C> cls) {
      for (FunctionContext context : contexts) {
        final C t = context.unwrap(cls);
        if (t != null) {
          return t;
        }
      }
      return null;
    }
  }

}
