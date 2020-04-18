package com.sdu.calcite.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sdu.calcite.function.FunctionKind;
import com.sdu.calcite.function.ScalarFunction;
import com.sdu.calcite.function.TableFunction;
import com.sdu.calcite.function.UserDefinedFunction;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

@Data
public abstract class SduFunction {

  private static final String CLASS_PROPERTY = "classpath";

  @JsonIgnore
  private SqlParserPos pos;

  private String name;

  private boolean deterministic;

  private Map<String, SduOption> properties;

  public abstract FunctionKind getKind();

  @SuppressWarnings("unchecked")
  public static SduFunction fromSqlCreateFunction(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCreateFunction) {
      SqlCreateFunction createFunction = (SqlCreateFunction) sqlNode;

      Map<String, SduOption> properties = createFunction.getProperties().getList()
          .stream()
          .map(SduOption::fromSqlOption)
          .collect(Collectors.toMap(SduOption::getKey, Function.identity()));
      SduOption option = properties.get(CLASS_PROPERTY);
      if (option == null) {
        throw new RuntimeException("Undefined function classpath, function: " + createFunction.getName());
      }
      String classpath = option.getValue();
      if (classpath == null || classpath.isEmpty()) {
        throw new RuntimeException("Undefined function classpath, function: " + createFunction.getName());
      }

      UserDefinedFunction definedFunction;
      try {
        Class<? extends UserDefinedFunction> cls = (Class<? extends UserDefinedFunction>) Class.forName(classpath);
        definedFunction = cls.newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Can'f initialize function instance, function: " + createFunction.getName(), e);
      }

      switch (definedFunction.getKind()) {
        case TABLE:
          TableFunction<?> tableFunction = (TableFunction<?>) definedFunction;
          return SduTableFunction.fromUserDefinedFunction(tableFunction);

        case SCALAR:
          ScalarFunction scalarFunction = (ScalarFunction) definedFunction;
          SduScalarFunction sduScalarFunction = SduScalarFunction.fromUserDefinedFunction(scalarFunction);
          sduScalarFunction.setPos(createFunction.getParserPosition());
          sduScalarFunction.setName(createFunction.getName().getSimple());
          sduScalarFunction.setProperties(properties);
          return sduScalarFunction;

        case AGGREGATE:
          AggregateFunction aggregateFunction = (AggregateFunction) definedFunction;
          return SduAggregateFunction.fromUserDefinedFunction(aggregateFunction);

        default:
          throw new UnsupportedOperationException("Unsupported function kind, kind: " + definedFunction.getKind());
      }

    }

    throw new IllegalArgumentException("SqlNode should be 'SqlCreateFunction' type");
  }
}
