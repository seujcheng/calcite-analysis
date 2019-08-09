package com.sdu.calcite.utils;

import com.sdu.calcite.plan.FeatureRelBuilder;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class CalciteSqlUtils {

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        if (schema.getParentSchema() == null) {
            return schema;
        }
        return rootSchema(schema.getParentSchema());
    }

    public static CalciteCatalogReader createCatalogReader(SchemaPlus defaultSchema, RelDataTypeFactory typeFactory, SqlParser.Config parserConfig) {
        SchemaPlus rootSchema = rootSchema(defaultSchema);

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(parserConfig.caseSensitive()));

        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(defaultSchema).path(null),
                typeFactory,
                new CalciteConnectionConfigImpl(props));
    }

    private static CalciteCatalogReader createCatalogReader(
            CalciteSchema rootSchema, List<String> defaultSchema, RelDataTypeFactory typeFactory, SqlParser.Config parserConfig) {
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(parserConfig.caseSensitive()));

        return new CalciteCatalogReader(
                rootSchema, defaultSchema, typeFactory, new CalciteConnectionConfigImpl(props));
    }

    public static RelOptCluster createRelOptCluster(RelOptPlanner planner, RexBuilder rexBuilder) {
        return RelOptCluster.create(planner, rexBuilder);
    }

    public static FeatureRelBuilder createRelBuilder(FrameworkConfig frameworkConfig) {

        RelDataTypeSystem typeSystem = frameworkConfig.getTypeSystem();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(typeSystem);

        VolcanoPlanner planner = new VolcanoPlanner();
        // 转换特征
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = createRelOptCluster(planner, new RexBuilder(typeFactory));

        CalciteSchema calciteSchema = CalciteSchema.from(frameworkConfig.getDefaultSchema());

        CalciteCatalogReader relOptSchema = createCatalogReader(
                calciteSchema, Collections.emptyList(), typeFactory, frameworkConfig.getParserConfig());

        return new FeatureRelBuilder(frameworkConfig.getContext(), cluster, relOptSchema);

    }

    public static RelNode runHepPlannerSequentially(
            HepMatchOrder hepMatchOrder, RuleSet ruleSet, RelNode input, RelTraitSet targetTraits, Context context) {

        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(hepMatchOrder);

        Iterator<RelOptRule> it = ruleSet.iterator();
        while (it.hasNext()) {
            builder.addRuleInstance(it.next());
        }

        return runHepPlanner(builder.build(), input, targetTraits, context);
    }

    private static RelNode runHepPlanner(HepProgram hepProgram, RelNode input, RelTraitSet targetTraits, Context cxt) {
        HepPlanner planner = new HepPlanner(hepProgram, cxt);
        planner.setRoot(input);
        if (input.getTraitSet() != targetTraits) {
            planner.changeTraits(input, targetTraits.simplify());
        }
        return planner.findBestExp();
    }

}
