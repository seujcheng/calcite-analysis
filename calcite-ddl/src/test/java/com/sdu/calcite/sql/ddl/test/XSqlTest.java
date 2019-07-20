package com.sdu.calcite.sql.ddl.test;

import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import com.sdu.calcite.sql.ddl.SqlJobDefine;
import com.sdu.calcite.sql.ddl.SqlUseFunction;
import com.sdu.calcite.sql.parser.XSqlParser;
import org.junit.Test;

public class XSqlTest {


    @Test
    public void testCreateFunction() throws Exception {
        String sql = "CREATE FUNCTION IS_IN_NYC AS 'com.sdu.sql.udf.IsInNYC' " +
                     "WITH (" +
                            " groupId = 'com.sdu.sql.udf'," +
                            " artifactId = 'udf'," +
                            " maven_version = '1.0.0')";

        SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) XSqlParser.parseOne(sql);

        System.out.println(sqlCreateFunction.getFunctionName());
        System.out.println(sqlCreateFunction.getFunctionClass());
        System.out.println(sqlCreateFunction.getFunctionProperties());
    }

    @Test
    public void testUseFunction() throws Exception {
        String sql = "USE FUNCTION IS_IN_NYC AS 'com.sdu.sql.udf.IsInNYC' " +
                     "WITH (" +
                     " maven_version = '1.0.0')";

        SqlUseFunction sqlUseFunction = (SqlUseFunction) XSqlParser.parseOne(sql);

        System.out.println(sqlUseFunction.getFunctionName());
        System.out.println(sqlUseFunction.getFunctionClass());
        System.out.println(sqlUseFunction.getFunctionProperties());
    }


    @Test
    public void testDefineJob() throws Exception {
        String sql = "DEFINE JOB stream_job_collector SET " +
                     "checkpoint WITH (checkpoint_interval = 5, checkpoint_concurrent = 6), " +
                     "recover WITH (strategy = 'NONE')";

        SqlJobDefine sqlJobDefine = (SqlJobDefine) XSqlParser.parseOne(sql);
        System.out.println(sqlJobDefine.getJobName());
        System.out.println(sqlJobDefine.getProperties("checkpoint"));
        System.out.println(sqlJobDefine.getProperties("recover"));
    }
}
