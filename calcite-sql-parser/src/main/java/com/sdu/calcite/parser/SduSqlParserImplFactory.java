package com.sdu.calcite.parser;

import java.io.Reader;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

public class SduSqlParserImplFactory implements SqlParserImplFactory {

  @Override
  public SqlAbstractParserImpl getParser(Reader stream) {
    return null;
  }


}
