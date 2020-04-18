package com.sdu.calcite.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SduInsert {

  @JsonIgnore
  @Getter
  @Setter
  private SqlParserPos pos;
  @JsonIgnore
  @Getter
  @Setter
  private SqlNode sqlNode;

  @Setter
  @Getter
  private String sqlText;
  @Setter
  @Getter
  private String targetTable;
  @Getter
  private Set<String> fromTables;


  private void addFromTable(String fromTable) {
    if (fromTables == null) fromTables = new HashSet<>();
    fromTables.add(fromTable);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SduInsert sduInsert = (SduInsert) o;
    return pos.equals(sduInsert.pos) && sqlNode.equals(sduInsert.sqlNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pos, sqlNode);
  }

  private static void parseNode(SqlNode sqlNode, SduInsert insert, SqlElement element) {
    // INSERT INTO table_name SELECT ...
    SqlKind sqlKind = sqlNode.getKind();
    switch (sqlKind) {
      case INSERT:
        SqlInsert sqlInsert = (SqlInsert) sqlNode;
        SqlNode sqlTarget = sqlInsert.getTargetTable();
        SqlNode sqlSource = sqlInsert.getSource();
        insert.setTargetTable(sqlTarget.toString());
        parseNode(sqlSource, insert, element);
        break;

      case SELECT:
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        SqlNode sqlFrom = sqlSelect.getFrom();
        parseNode(sqlFrom, insert, SqlElement.TABLE);
        break;

      case JOIN:
        SqlJoin sqlJoin = (SqlJoin) sqlNode;
        SqlNode leftNode = sqlJoin.getLeft();
        SqlNode rightNode = sqlJoin.getRight();
        parseNode(leftNode, insert, SqlElement.TABLE);
        parseNode(rightNode, insert, SqlElement.TABLE);
        break;

      case UNION:
        SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];

        parseNode(unionLeft, insert, SqlElement.TABLE);
        parseNode(unionRight, insert, SqlElement.TABLE);
        break;

      case AS:
        SqlBasicCall asBasicCall = (SqlBasicCall) sqlNode;
        if (element == SqlElement.TABLE) {
          parseNode(asBasicCall.operand(0), insert, element);
        }
        break;

      case IDENTIFIER:
        SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
        if (element == SqlElement.TABLE) {
          insert.addFromTable(sqlIdentifier.getSimple());
        }
        break;

      default:
        if (sqlNode instanceof SqlBasicCall) {
          SqlBasicCall basicCall = (SqlBasicCall) sqlNode;
          for (SqlNode node : basicCall.operands) {
            parseNode(node, insert, element);
          }
        }
        break;
    }
  }

  private enum SqlElement {
    NONE,
    TABLE
  }

  public static SduInsert fromSqlInsert(SqlNode sqlNode) {
    if (sqlNode instanceof SqlInsert) {
      SqlInsert sqlInsert = (SqlInsert) sqlNode;

      SduInsert sduInsert = new SduInsert();
      sduInsert.setPos(sqlInsert.getParserPosition());
      sduInsert.setSqlNode(sqlInsert);
      sduInsert.setSqlText(sqlInsert.toString());
      parseNode(sqlInsert, sduInsert, SqlElement.NONE);

      return sduInsert;
    }
    throw new IllegalArgumentException("SqlNode should be 'SqlInsert' type");
  }

}
