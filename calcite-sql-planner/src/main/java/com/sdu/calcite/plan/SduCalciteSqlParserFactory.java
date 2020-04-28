package com.sdu.calcite.plan;

import com.sdu.calcite.sql.parser.SduSqlParserImpl;
import java.io.Reader;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

public class SduCalciteSqlParserFactory implements SqlParserImplFactory {

  @Override
  public SduSqlParserImpl getParser(Reader stream) {
    SqlParser.Config config = SqlParser.configBuilder()
        .setLex(Lex.JAVA)
        .setQuotedCasing(Lex.JAVA.quotedCasing)
        .setUnquotedCasing(Lex.JAVA.unquotedCasing)
        .build();

    SduSqlParserImpl parser = new SduSqlParserImpl(stream);
    parser.setTabSize(1);
    parser.setQuotedCasing(config.quotedCasing());
    parser.setUnquotedCasing(config.unquotedCasing());
    parser.setIdentifierMaxLength(config.identifierMaxLength());
    parser.setConformance(config.conformance());
    switch (config.quoting()) {
      case DOUBLE_QUOTE:
        parser.switchTo("DQID");
        break;
      case BACK_TICK:
        parser.switchTo("BTID");
        break;
      case BRACKET:
        parser.switchTo("DEFAULT");
        break;
      default:
        throw new IllegalArgumentException("Unsupported Quoting type: " + config.quoting());
    }
    return parser;
  }


}
