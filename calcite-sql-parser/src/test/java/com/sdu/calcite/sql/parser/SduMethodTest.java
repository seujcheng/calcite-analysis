package com.sdu.calcite.sql.parser;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class SduMethodTest {

  private static class ArgsMethods {

   public int add(int ... nums) {
      int sum = 0;
      for (int i = 0; i < nums.length; ++i) {
        sum += nums[i];
      }
      return sum;
    }

  }

  @Test
  public void testArgsMethods() {
    List<Method> methods = Arrays.stream(ArgsMethods.class.getMethods())
        .filter(m -> m.getName().equals("add"))
        .collect(Collectors.toList());
    Assert.assertEquals(1, methods.size());
    Method method = methods.get(0);
    System.out.println(method.getParameterTypes().length);

  }

}
