package com.sdu.sql.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sdu.calcite.function.AggregateFunction;
import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.ScalarFunction;
import com.sdu.calcite.function.TableFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

@Data
public abstract class SduFunction {

  private static final String CLASS_PATH = "classpath";
  private static final String CLASS_URL = "url";

  @JsonIgnore
  private SqlParserPos pos;

  private String name;

  private boolean deterministic;

  private Map<String, SduOption> properties;

  public abstract UserDefinedFunction getUserDefinedFunction();

  public abstract FunctionKind getKind();

  @SuppressWarnings("unchecked")
  public static SduFunction fromSqlCreateFunction(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCreateFunction) {
      SqlCreateFunction createFunction = (SqlCreateFunction) sqlNode;
      String functionName = createFunction.getName().getSimple();

      /*
       * CREATE FUNCTION DATA_FORMAT WITH (
       *    'url' = '',
       *    'classpath' = 'com.sdu.calcite.functions.DataFormat'
       * )
       * */
      Map<String, SduOption> properties = createFunction.getProperties().getList()
          .stream()
          .map(SduOption::fromSqlOption)
          .collect(Collectors.toMap(SduOption::getKey, Function.identity()));
      validateCreateFunctionProperties(functionName, properties);

      // TODO: UrlClassLoader load remote jar
      UserDefinedFunction definedFunction;
      try {
        Class<? extends UserDefinedFunction> cls = (Class<? extends UserDefinedFunction>) Class.forName(getFunctionClasspath(properties));
        definedFunction = cls.newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Can'f initialize function instance, function: " + createFunction.getName(), e);
      }

      switch (definedFunction.getKind()) {
        case TABLE:
          TableFunction<?> tableFunction = (TableFunction<?>) definedFunction;
          SduTableFunction sduTableFunction = SduTableFunction.fromUserDefinedFunction(tableFunction);
          sduTableFunction.setPos(createFunction.getParserPosition());
          sduTableFunction.setName(createFunction.getName().getSimple());
          sduTableFunction.setProperties(properties);
          return sduTableFunction;

        case SCALAR:
          ScalarFunction scalarFunction = (ScalarFunction) definedFunction;
          SduScalarFunction sduScalarFunction = SduScalarFunction.fromUserDefinedFunction(scalarFunction);
          sduScalarFunction.setPos(createFunction.getParserPosition());
          sduScalarFunction.setName(createFunction.getName().getSimple());
          sduScalarFunction.setProperties(properties);
          return sduScalarFunction;

        case AGGREGATE:
          AggregateFunction<?, ?> aggregateFunction = (AggregateFunction<?, ?>) definedFunction;
          return SduAggregateFunction.fromUserDefinedFunction(aggregateFunction);

        default:
          throw new UnsupportedOperationException("Unsupported function kind, kind: " + definedFunction.getKind());
      }

    }

    throw new IllegalArgumentException("SqlNode should be 'SqlCreateFunction' type");
  }

  private static void validateCreateFunctionProperties(String functionName, Map<String, SduOption> properties) {
    SduOption urlOption = properties.get(CLASS_URL);
    if (urlOption == null) {
      throw new RuntimeException("Undefined function url, function: " + functionName);
    }

    SduOption classOption = properties.get(CLASS_PATH);
    if (classOption == null) {
      throw new RuntimeException("Undefined function classpath, function: " + functionName);
    }
    String classpath = classOption.getValue();
    if (StringUtils.isEmpty(classpath)) {
      throw new RuntimeException("Undefined function classpath, function: " + functionName);
    }
  }

  private static String getFunctionClasspath(Map<String, SduOption> properties) {
    return properties.get(CLASS_PATH).getValue();
  }

  private static String getFunctionUrl(Map<String, SduOption> properties) {
    return properties.get(CLASS_URL).getValue();
  }

}
