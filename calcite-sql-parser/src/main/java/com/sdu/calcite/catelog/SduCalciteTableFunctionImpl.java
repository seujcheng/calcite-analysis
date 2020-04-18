package com.sdu.calcite.catelog;

import static java.util.Objects.requireNonNull;

import com.sdu.calcite.types.SduCalciteTypeFactory;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

public class SduCalciteTableFunctionImpl implements TableFunction {

  private final String[] columnNames;
  private final Class<?>[] columnTypes;

  public SduCalciteTableFunctionImpl(String[] columnNames, Class<?>[] columnTypes) {
    requireNonNull(columnNames);
    requireNonNull(columnTypes);
    if (columnNames.length != columnTypes.length) {
      throw new IllegalArgumentException("Number of field names and field types must be equal.");
    }
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
    SduCalciteTypeFactory sduTypeFactory = (SduCalciteTypeFactory) typeFactory;
    FieldInfoBuilder builder = sduTypeFactory.builder();
    for (int i = 0; i < columnNames.length; ++i) {
      builder.add(columnNames[i], sduTypeFactory.createSqlType(columnTypes[i].getName()));
    }
    return builder.build();
  }

  @Override
  public Type getElementType(List<Object> arguments) {
    return Object[].class;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return Collections.emptyList();
  }

}
