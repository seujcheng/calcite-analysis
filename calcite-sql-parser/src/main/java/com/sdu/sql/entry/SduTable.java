package com.sdu.sql.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

@Data
public class SduTable {

  @JsonIgnore
  private SqlParserPos pos;

  private String name;

  private List<SduTableColumn> columns;

  private String comment;

  private Map<String, SduOption> properties;


  public static SduTable fromSqlCreateTable(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCreateTable) {
      SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;

      SduTable table = new SduTable();
      table.setPos(sqlCreateTable.getParserPosition());
      table.setName(sqlCreateTable.getName().getSimple());
      SqlCharStringLiteral comment = sqlCreateTable.getComment();
      if (comment != null) {
        table.setComment(comment.getNlsString().getValue());
      }
      
      List<SduTableColumn> columns = sqlCreateTable.getColumns().getList()
          .stream()
          .map(SduTableColumn::fromSqlTableColumn)
          .collect(Collectors.toList());
      table.setColumns(columns);

      SqlNodeList properties = sqlCreateTable.getProperties();
      if (properties != null) {
        Map<String, SduOption> props = properties.getList()
            .stream()
            .map(SduOption::fromSqlOption)
            .collect(Collectors.toMap(SduOption::getKey, Function.identity()));
        table.setProperties(props);
      }
      return table;
    }
    throw new IllegalArgumentException("SqlNode should be 'SqlCreateTable' type");
  }

}
