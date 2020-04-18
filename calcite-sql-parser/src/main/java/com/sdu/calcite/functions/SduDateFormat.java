package com.sdu.calcite.functions;

import com.sdu.calcite.function.ScalarFunction;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

public class SduDateFormat extends ScalarFunction {

  public String eval(Timestamp time, String pattern) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    return formatter.format(time.toLocalDateTime());
  }

}
