package com.sdu.calcite.sql.ddl;

import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlTableColumn extends SqlCall {

  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("COLUMN_DESC", SqlKind.COLUMN_DECL);

  private SqlIdentifier name;
  private SqlDataTypeSpec type;
  private SqlCharStringLiteral comment;

  public SqlTableColumn(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec type, SqlCharStringLiteral comment) {
    super(pos);
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public String getName() {
    return name.getSimple();
  }

  public String getComment() {
    return comment.getNlsString().getValue();
  }

  public RelDataType getType(RelDataTypeFactory typeFactory) {
    return type.deriveType(typeFactory);
  }

  @Nonnull
  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, type, comment);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    writer.print(" ");
    type.unparse(writer, leftPrec, rightPrec);
    if (this.comment != null) {
      writer.print(" COMMENT ");
      comment.unparse(writer, leftPrec, rightPrec);
    }
  }

}
