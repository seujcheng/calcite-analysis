package com.sdu.calcite.plan.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import com.sdu.calcite.plan.nodes.FeatureTableScan;

import java.util.LinkedList;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class PushProjectIntoTableScanRule extends RelOptRule {

    public static final PushProjectIntoTableScanRule INSTANCE = new PushProjectIntoTableScanRule();

    private PushProjectIntoTableScanRule() {
        // LogicalCalc
        //   FeatureTableScan
        super(operand(LogicalCalc.class, operand(FeatureTableScan.class, none())),
                "PushProjectIntoTableScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FeatureTableScan scan = call.rel(1);
        return scan.featureNamesEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        FeatureTableScan scan = call.rel(1);

        List<Pair<RexNode, String>> nameProjects = project.getNamedProjects();
        List<String> featureNames = new LinkedList<>();
        for (Pair<RexNode, String> p : nameProjects) {
            featureNames.add(p.right);
        }

        scan = FeatureTableScan.create(scan.getCluster(), scan.getTraitSet(), scan.getTable(), featureNames);

        LogicalProject newProject = project.copy(project.getTraitSet(), scan, project.getProjects(), project.getRowType());


        call.transformTo(newProject);

    }
}
