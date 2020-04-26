package com.sdu.calcite.plan.nodes;

import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

public interface SduLogicalRel extends RelNode {

  default double estimateRowSize(RelDataType rowType) {
    return rowType.getFieldList()
        .stream()
        .map(f -> estimateDataTypeSize(f.getType()))
        .mapToDouble(x -> x)
        .sum();
  }

  default double estimateDataTypeSize(RelDataType t) {
    switch (t.getSqlTypeName()) {
      case TINYINT:
        return 1.0;
      case SMALLINT:
        return 2.0;
      case INTEGER:
        return 4.0;
      case BIGINT:
        return 8.0;
      case BOOLEAN:
        return 1.0;
      case FLOAT:
        return 4.0;
      case DOUBLE:
        return 8.0;
      case VARCHAR:
        return 12.0;
      case CHAR:
        return 1.0;
      case DECIMAL:
        return 12.0;
      case TIME:
      case DATE:
      case TIMESTAMP:
        return 12.0;
      case ROW:
        return estimateRowSize(t);
      case ARRAY:
        return estimateDataTypeSize(t.getComponentType()) * 16;
      case MAP:
      case MULTISET:
        return estimateDataTypeSize(t.getKeyType()) + estimateDataTypeSize(t.getValueType()) * 16;
      case ANY:
        return 128.0;
      default:
        if (YEAR_INTERVAL_TYPES.contains(t.getSqlTypeName())) {
          return 8.0;
        }
        if (DAY_INTERVAL_TYPES.contains(t.getSqlTypeName())) {
          return 4.0;
        }
        throw new UnsupportedOperationException("Unsupported date type, type: " + t);
    }
  }

}
