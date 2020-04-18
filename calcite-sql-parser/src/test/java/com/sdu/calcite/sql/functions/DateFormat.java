package com.sdu.calcite.sql.functions;

import com.sdu.calcite.function.ScalarFunction;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

public class DateFormat extends ScalarFunction {

  public String eval(Timestamp timestamp, String pattern) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    return formatter.format(timestamp.toLocalDateTime());
  }

}
