package com.sdu.calcite.sql.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.io.StringReader;

/**
 * @author hanhan.zhang
 * */
public class XSqlParser {

    public static SqlNodeList parse(String text) throws Exception {
        try (StringReader in = new StringReader(text)) {
            XSqlParserImpl parser = new XSqlParserImpl(in);

            // back tick as the quote
            parser.switchTo("BTID");
            parser.setTabSize(1);
            parser.setQuotedCasing(Lex.JAVA.quotedCasing);
            parser.setUnquotedCasing(Lex.JAVA.unquotedCasing);
            parser.setIdentifierMaxLength(128);
            return parser.parseSqlStmtList();
        }
    }

    public static SqlNode parseOne(String text) throws Exception {
        return parse(text).get(0);
    }
}
