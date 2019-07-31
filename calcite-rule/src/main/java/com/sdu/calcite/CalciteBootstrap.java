package com.sdu.calcite;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
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

import com.sdu.calcite.plan.FeaturePlanner;
import com.sdu.calcite.plan.FeatureRelBuilder;
import com.sdu.calcite.plan.FeatureSqlValidator;
import com.sdu.calcite.plan.FeatureTableEnvironment;
import com.sdu.calcite.plan.rules.FeatureTableScanRule;
import com.sdu.calcite.plan.rules.PushProjectIntoTableScanRule;
import com.sdu.calcite.table.FeatureTable;
import com.sdu.calcite.utils.CalciteSqlUtils;

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
//        final String sql = "SELECT" +
//                           " p.poi_id, label, deal_id, name " +
//                           "FROM " +
//                           " tps_domain p " +
//                           "JOIN tqs_domain q ON p.poi_id = q.poi_id " +
//                           "WHERE " +
//                           " deal_id = 11";

        final String sql = "SELECT poi_id, label FROM tps_domain";

        // 定义表
        FeatureTable poiTable = new FeatureTable(
                new String[] {"poi_id", "label"},
                new Class<?>[] {Long.class, String.class});
        FeatureTable dealTable = new FeatureTable(
                new String[] {"deal_id", "poi_id", "name"},
                new Class<?>[] {Long.class, Long.class, String.class});

        //
        FeatureTableEnvironment tableEnv = new FeatureTableEnvironment();
        tableEnv.registerTable("tps_domain", poiTable);
        tableEnv.registerTable("tqs_domain", dealTable);

        FrameworkConfig frameworkConfig = tableEnv.frameworkConfig();

        FeaturePlanner planner = new FeaturePlanner(frameworkConfig, tableEnv.getPlanner(), tableEnv.getTypeFactory());

        // step1: 解析构建抽象语法树(AST)
        SqlNode sqlNode = planner.parse(sql);

        // step2: 校验(表名, 字段)
        SqlNode validateSqlNode = planner.validate(sqlNode);

        // step3: 语义分析(SqlNode -> RelNode)
        RelRoot root = planner.rel(validateSqlNode);

        System.out.println(RelOptUtil.toString(root.rel, SqlExplainLevel.NON_COST_ATTRIBUTES));

        // step4: 优化执行计划
        // HepPlanner采用Greedy算法(贪婪算法), 将每次运行的Rule使用HepProgram进行封装.
        // HepProgram由不同的HepInstruction组成, 按照HepInstruction加入到HepProgram顺序执行(简单循环, 详见源码)
        // HepInstruction有MatchLimit、MatchOrder、Rules组成
        //     MatchLimit表示这次HepProgram优化的次数限制, 如果不设置则为无穷(防止死循环)
        //     MatchOrder则表示每次rule执行的顺序: ARBITRARY、BOTTOM_UP、TOP_DOWN三种方式, 其中ARBITRARY被认为是最高效的apply方式
        HepProgramBuilder programBuilder = new HepProgramBuilder()
                .addRuleInstance(FeatureTableScanRule.INSTANCE)
                // 先转为FeatureTableScan, '谓词下推'
                .addRuleInstance(PushProjectIntoTableScanRule.INSTANCE)
                .addMatchLimit(10)
                .addMatchOrder(HepMatchOrder.TOP_DOWN);
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build(), frameworkConfig.getContext());
        hepPlanner.setRoot(root.rel);
        RelNode optimizeRelNode = hepPlanner.findBestExp();

        System.out.println(RelOptUtil.toString(optimizeRelNode, SqlExplainLevel.NON_COST_ATTRIBUTES));
    }

}
