package com.sdu.calcite.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

@Data
public class SduFunction {

  @JsonIgnore
  private SqlParserPos pos;

  private String name;

  private Map<String, SduOption> properties;


  public static SduFunction fromSqlCreateFunction(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCreateFunction) {
      SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNode;

      SduFunction function = new SduFunction();
      function.setPos(sqlCreateFunction.getParserPosition());
      function.setName(sqlCreateFunction.getName().getSimple());

      SqlNodeList properties = sqlCreateFunction.getProperties();
      if (properties != null) {
        Map<String, SduOption> props = properties.getList()
            .stream()
            .map(SduOption::fromSqlOption)
            .collect(Collectors.toMap(SduOption::getKey, Function.identity()));
        function.setProperties(props);
      }
      return function;
    }

    throw new IllegalArgumentException("SqlNode should be 'SqlCreateFunction' type");
  }
}
