package com.sdu.calcite.sql.ddl;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlWatermark extends SqlCall {

  private static final SqlOperator OPERATOR = new SqlSpecialOperator("WATERMARK", SqlKind.OTHER);

  private final SqlIdentifier eventTimeColumn;
  private final SqlNode strategy;

  public SqlWatermark(SqlParserPos pos, SqlIdentifier eventTimeColumn, SqlNode strategy) {
    super(pos);
    this.eventTimeColumn = Objects.requireNonNull(eventTimeColumn);
    this.strategy = Objects.requireNonNull(strategy);
  }

  @Nonnull
  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(eventTimeColumn, strategy);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("WATERMARK FOR");
    eventTimeColumn.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    strategy.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getEventTimeColumn() {
    return eventTimeColumn;
  }

  public SqlNode getStrategy() {
    return strategy;
  }

}
