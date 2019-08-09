package com.sdu.calcite.plan;

import com.sdu.calcite.feature.fetcher.FeatureGetter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * @author hanhan.zhang
 * */
public interface FeatureRel extends RelNode {

    Convention CONVENTION = new Convention.Impl("Feature", FeatureRel.class);

    FeatureGetter translateToFeatureGetter();

}
