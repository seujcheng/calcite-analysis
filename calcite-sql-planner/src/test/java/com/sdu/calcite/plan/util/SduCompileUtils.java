package com.sdu.calcite.plan.util;

import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SduCompileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SduCompileUtils.class);

  private SduCompileUtils() {

  }

  @SuppressWarnings("unchecked")
  public static <T> Class<T> compile(ClassLoader cl, String name, String code) {
    LOG.info("Compiling: {} \n\n Code: \n{}", name, code);
    SimpleCompiler compiler = new SimpleCompiler();
    compiler.setParentClassLoader(cl);
    try {
      compiler.cook(code);
    } catch (Throwable t) {
      throw new IllegalStateException(
          "Table program cannot be compiled. This is a bug. Please file an issue.", t);
    }
    try {
      //noinspection unchecked
      return (Class<T>) compiler.getClassLoader().loadClass(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can not load class " + name, e);
    }
  }

}
