package com.sdu.calcite;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.plan.nodes.Conventions;
import com.sdu.calcite.plan.rules.FeatureTableScanRule;
import com.sdu.calcite.table.FeatureStreamTable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;

import java.util.List;
import java.util.Properties;

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

        FeatureStreamTable table = new FeatureStreamTable(
                new String[] {"poi_id", "label"},
                new Class<?>[] {Long.class, String.class});

        // 元数据信息
        SchemaPlus defaultSchema = Frameworks.createRootSchema(true);
        defaultSchema.add("tps_domain", table);


        // step1: 解析构建抽象语法树(AST)
        SqlParser.Config config = SqlParser.configBuilder()
                .setUnquotedCasing(Casing.UNCHANGED)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseStmt();

        System.out.println(sqlNode.getKind());

        // step2: 校验(表名, 字段)
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(defaultSchema),
                CalciteSchema.from(defaultSchema).path(null), typeFactory, new CalciteConnectionConfigImpl(new Properties()));
        SqlValidator sqlValidator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader,
                typeFactory, SqlConformanceEnum.DEFAULT);

        SqlNode validateSqlNode = sqlValidator.validate(sqlNode);

        // step3: 语义分析(SqlNode -> RelNode)
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        RelOptPlanner planner = new HepPlanner(hepProgramBuilder.build());

        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        final SqlToRelConverter.Config conf = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .build();
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new SimpleViewExpander(),
                sqlValidator, catalogReader, cluster, StandardConvertletTable.INSTANCE, conf);

        RelRoot root = sqlToRelConverter.convertQuery(validateSqlNode, false, true);

        final RelBuilder relBuilder = conf.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));

        RelNode relNode = root.rel;

        // step4: 优化执行计划
        Program program = Programs.ofRules(new FeatureTableScanRule());

        relNode = program.run(planner, relNode, RelTraitSet.createEmpty(), ImmutableList.of(), ImmutableList.of());

        RelJsonWriter writer = new RelJsonWriter();
        relNode.explain(writer);
        System.out.println(writer.asString());
    }

}
