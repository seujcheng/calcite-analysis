package com.sdu.calcite;

import com.sdu.calcite.plan.FeatureRelBuilder;
import com.sdu.calcite.plan.FeatureSqlValidator;
import com.sdu.calcite.plan.rules.FeatureTableScanRule;
import com.sdu.calcite.table.FeatureStreamTable;
import com.sdu.calcite.utils.CalciteSqlUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.List;

import static java.lang.Integer.MAX_VALUE;

/**
 * @author hanhan.zhang
 * */
public class CalciteBootstrap {



    public static class SimpleViewExpander implements RelOptTable.ViewExpander {

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
            return null;
        }

    }

    public static void main(String[] args) throws Exception {
        final String sql = "SELECT poi_id, label FROM tps_domain";

        // 元数据
        SchemaPlus defaultSchema = CalciteSchema
                .createRootSchema(false, false)
                .plus();

        FeatureStreamTable table = new FeatureStreamTable(
                new String[] {"poi_id", "label"},
                new Class<?>[] {Long.class, String.class});
        defaultSchema.add("tps_domain", table);

        SqlToRelConverter.Config toRelConvertConfig = SqlToRelConverter.configBuilder()
                // TableScan --> LogicalTableScan
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(MAX_VALUE)
                .build();
        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(defaultSchema)
                .parserConfig(SqlParser.configBuilder().setLex(Lex.JAVA).build())
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .operatorTable(SqlStdOperatorTable.instance())
                .sqlToRelConverterConfig(toRelConvertConfig)
                .build();


        // step1: 解析构建抽象语法树(AST)
        SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
        SqlNode sqlNode = parser.parseStmt();

        FeatureRelBuilder relBuilder = CalciteSqlUtils.createRelBuilder(frameworkConfig);

        // step2: 校验(表名, 字段)
        CalciteCatalogReader catalogReader = CalciteSqlUtils.createCatalogReader(
                defaultSchema, relBuilder.getTypeFactory(), frameworkConfig.getParserConfig());
        SqlValidatorImpl sqlValidator = new FeatureSqlValidator(frameworkConfig.getOperatorTable(),
                catalogReader, relBuilder.getTypeFactory());
        SqlNode validateSqlNode = sqlValidator.validate(sqlNode);

        // step3: 语义分析(SqlNode -> RelNode)
        RexBuilder rexBuilder = new RexBuilder(relBuilder.getTypeFactory());
        RelOptCluster cluster = CalciteSqlUtils.createRelOptCluster(relBuilder.getPlaner(), rexBuilder);
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                new SimpleViewExpander(), sqlValidator, catalogReader, cluster, frameworkConfig.getConvertletTable(),
                frameworkConfig.getSqlToRelConverterConfig());

        RelRoot root = sqlToRelConverter.convertQuery(validateSqlNode, false, true);

        System.out.println(RelOptUtil.toString(root.rel, SqlExplainLevel.NON_COST_ATTRIBUTES));

        // step4: 优化执行计划
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(FeatureTableScanRule.INSTANCE);
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build(), frameworkConfig.getContext());
        hepPlanner.setRoot(root.rel);
        RelNode optimizeRelNode = hepPlanner.findBestExp();

        System.out.println(RelOptUtil.toString(optimizeRelNode, SqlExplainLevel.NON_COST_ATTRIBUTES));
    }

}
