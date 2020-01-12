package com.sdu.calcite.sql.planner;

import com.sdu.calcite.sql.parser.XSqlParserImpl;
import java.io.Reader;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

public class XSqlParserImplFactory implements SqlParserImplFactory {

  @Override
  public SqlAbstractParserImpl getParser(Reader stream) {
    return new XSqlParserImpl(stream);
  }

}
