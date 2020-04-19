package com.sdu.sql.parse;

import com.sdu.sql.entry.SduFunction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SduFunctionCatalog {

  private final Map<String, SduFunction> userDefinedFunctions;

  public SduFunctionCatalog() {
    userDefinedFunctions = new HashMap<>();
  }

  public void registerUserDefinedFunction(String name, SduFunction function) {
    SduFunction oldFunction = userDefinedFunctions.put(name, function);
    if (oldFunction != null) {
      userDefinedFunctions.put(name, oldFunction);
      throw new RuntimeException("Define duplicate function name: " + name);
    }
  }

  public Optional<SduFunction> lookupFunction(String name) {
    return userDefinedFunctions.containsKey(name) ? Optional.of(userDefinedFunctions.get(name)) : Optional.empty();
  }

}
