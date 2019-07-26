package com.sdu.calcite.plan;

import com.sdu.calcite.feature.FeatureStream;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * @author hanhan.zhang
 * */
public interface FeatureStreamRel extends RelNode {

    Convention CONVENTION = new Convention.Impl("FeatureStream", FeatureStreamRel.class);

    FeatureStream convertTo();

}
