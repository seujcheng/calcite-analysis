package com.sdu.calcite.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sdu.calcite.sql.ddl.SqlTableColumn;
import lombok.Data;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

@Data
public class SduTableColumn {

  @JsonIgnore
  private SqlParserPos pos;

  private String name;
  private String type;
  private String path;
  private String comment;


  static SduTableColumn fromSqlTableColumn(SqlNode sqlNode) {
    if (sqlNode instanceof SqlTableColumn) {
      SqlTableColumn sqlTableColumn = (SqlTableColumn) sqlNode;

      SduTableColumn sduTableColumn = new SduTableColumn();
      sduTableColumn.setPos(sqlTableColumn.getParserPosition());
      sduTableColumn.setName(sqlTableColumn.getName().getSimple());
      sduTableColumn.setType(sqlTableColumn.dataType());
      SqlCharStringLiteral path = sqlTableColumn.getPath();
      if (path != null) {
        sduTableColumn.setPath(path.getNlsString().getValue());
      }
      SqlCharStringLiteral comment = sqlTableColumn.getComment();
      if (comment != null) {
        sduTableColumn.setComment(comment.getNlsString().getValue());
      }
      return sduTableColumn;
    }
    throw new IllegalArgumentException("SqlNode should be 'SqlTableColumn' type");
  }

}
