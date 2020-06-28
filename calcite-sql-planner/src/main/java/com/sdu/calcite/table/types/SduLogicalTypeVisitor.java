package com.sdu.calcite.table.types;

public interface SduLogicalTypeVisitor<R> {

  R visit(SduCharType charType);

  R visit(SduVarCharType varCharType);

  R visit(SduBooleanType booleanType);

  R visit(SduTinyIntType tinyIntType);

  R visit(SduSmallIntType smallIntType);

  R visit(SduIntType intType);

  R visit(SduBigIntType bigIntType);

  R visit(SduFloatType floatType);

  R visit(SduDoubleType doubleType);

  R visit(SduRowType rowType);
}
