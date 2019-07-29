package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.FeatureRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

/**
 * @author hanhan.zhang
 * */
public class FeatureProjectRule extends ConverterRule {

    public static final FeatureProjectRule INSTANCE = new FeatureProjectRule();

    private FeatureProjectRule() {
        super(LogicalProject.class, Convention.NONE, FeatureRel.CONVENTION, "FeatureTableProject");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        System.out.println(rel);
        return null;
    }

}
