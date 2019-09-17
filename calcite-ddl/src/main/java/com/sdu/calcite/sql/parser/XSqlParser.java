package com.sdu.calcite.sql.parser;

import java.io.Reader;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

/**
 * @author hanhan.zhang
 * */
public class XSqlParser {

    private static final SqlParser.Config config;

    static {
        config = SqlParser.configBuilder()
                .setCaseSensitive(true)
                // 禁止转为大写
                .setUnquotedCasing(Casing.UNCHANGED)
                .setParserFactory(new SqlParserImplFactoryImpl())
                .build();
    }


    private static class SqlParserImplFactoryImpl implements SqlParserImplFactory {

        private SqlParserImplFactoryImpl() {}

        @Override
        public SqlAbstractParserImpl getParser(Reader stream) {
            return new XSqlParserImpl(stream);
        }

    }

    public static SqlNodeList parse(String text) throws Exception {
        SqlParser sqlParser = SqlParser.create(text, config);
        return sqlParser.parseStmtList();
    }

    public static SqlNode parseOne(String text) throws Exception {
        return parse(text).get(0);
    }

}
