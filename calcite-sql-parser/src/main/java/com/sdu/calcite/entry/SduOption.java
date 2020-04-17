package com.sdu.calcite.entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sdu.calcite.sql.ddl.SqlOption;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

@Data
public class SduOption {

  @JsonIgnore
  private SqlParserPos pos;

  private String key;

  private String value;


  static SduOption fromSqlOption(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOption) {
      SqlOption sqlOption = (SqlOption) sqlNode;

      SduOption sduOption = new SduOption();
      sduOption.setPos(sqlOption.getParserPosition());
      sduOption.setKey(sqlOption.getKeyString());
      sduOption.setValue(sqlOption.getValueString());

      return sduOption;
    }
    throw new IllegalArgumentException("SqlNode should be 'SqlOption' type");
  }

}
