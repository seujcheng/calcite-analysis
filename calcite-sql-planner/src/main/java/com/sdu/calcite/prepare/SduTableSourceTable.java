package com.sdu.calcite.prepare;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;

public class SduTableSourceTable extends SduPreparingTable {

  SduTableSourceTable(
      RelOptSchema relOptSchema,
      RelDataType rowType,
      List<String> names, // catalog, database, table
      Statistic statistic) {
    super(relOptSchema, rowType, names, statistic);
  }

  public static SduTableSourceTable of(SduCatalogSourceTable sourceTable,  Integer[] selectedFields) {
    RelDataTypeFactory typeFactory = sourceTable.getRelOptSchema().getTypeFactory();
    List<RelDataTypeField> originalFieldTypes = sourceTable.getRowType().getFieldList();

    List<String> fieldNames = new ArrayList<>();
    List<RelDataType> fieldTypes = new ArrayList<>();
    Arrays.stream(selectedFields)
        .forEach(idx -> {
          RelDataTypeField field = originalFieldTypes.get(idx);
          fieldNames.add(field.getName());
          fieldTypes.add(field.getType());
        });
    RelDataType newRowDataType = typeFactory.createStructType(fieldTypes, fieldNames);

    return new SduTableSourceTable(sourceTable.getRelOptSchema(),
        newRowDataType,
        sourceTable.getQualifiedName(),
        sourceTable.getStatistic());

  }
}
