package com.sdu.calcite.sql.ddl;

import com.sdu.calcite.sql.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.*;

/**
 * DEFINE JOB job_name SET key WITH (name = value [, name = value]*) [, key WITH (name = value [, name = value]*)]
 *
 * @author hanhan.zhang
 * */
public class SqlJobDefine extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DEFINE JOB", SqlKind.OTHER_DDL);

    private SqlIdentifier jobName;
    private Map<String, SqlNodeList> jobConf;

    public SqlJobDefine(SqlParserPos pos, SqlIdentifier jobName, Map<String, SqlNodeList> jobConf) {
        super(OPERATOR, pos);

        this.jobName = jobName;
        this.jobConf = jobConf;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DEFINE");
        writer.keyword("JOB");
        jobName.unparse(writer, leftPrec, rightPrec);
        if (jobConf != null) {
            writer.keyword("SET");
            for (Map.Entry<String, SqlNodeList> entry : jobConf.entrySet()) {
                writer.literal(entry.getKey());
                SqlUtils.unparse(writer, entry.getValue(), "WITH");
            }
        }
    }

    @Override
    public SqlOperator getOperator() {
        return super.getOperator();
    }

    @Override
    public List<SqlNode> getOperandList() {
       List<SqlNode> operandList = new ArrayList<>();
       operandList.add(jobName);
       operandList.addAll(jobConf.values());
       return operandList;
    }

    public String getJobName() {
        return jobName.toString();
    }

    public Map<String, String> getProperties(String key) {
        if (jobConf == null || !jobConf.containsKey(key)) {
            return Collections.emptyMap();
        }

        Map<String, String> props = new HashMap<>();
        for (SqlNode node : jobConf.get(key)) {
            SqlPropertyNode propertyNode = (SqlPropertyNode) node;
            props.put(propertyNode.getPropertyName(), propertyNode.getPropertyValue());
        }
        return props;
    }
}
