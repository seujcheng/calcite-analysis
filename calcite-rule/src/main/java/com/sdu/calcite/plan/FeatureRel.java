package com.sdu.calcite.plan;

import com.sdu.calcite.feature.FeatureContext;
import com.sdu.calcite.feature.FeatureData;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * @author hanhan.zhang
 * */
public interface FeatureRel extends RelNode {

    Convention CONVENTION = new Convention.Impl("Feature", FeatureRel.class);

    FeatureData convertTo(FeatureContext ctx);

}
