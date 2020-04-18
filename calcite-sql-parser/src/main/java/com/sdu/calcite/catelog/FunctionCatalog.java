package com.sdu.calcite.catelog;

import com.sdu.calcite.entry.SduFunction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FunctionCatalog {

  private final Map<String, SduFunction> userDefinedFunctions;

  public FunctionCatalog() {
    userDefinedFunctions = new HashMap<>();
  }

  public void registerUserDefinedFunction(String name, SduFunction function) {
    userDefinedFunctions.put(name, function);
  }

  Optional<SduFunction> lookupFunction(String name) {
    return userDefinedFunctions.containsKey(name) ? Optional.of(userDefinedFunctions.get(name)) : Optional.empty();
  }

}
