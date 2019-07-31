package com.sdu.calcite.plan;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import com.sdu.calcite.feature.FeatureContext;
import com.sdu.calcite.feature.FeatureData;

import java.util.concurrent.CompletableFuture;

/**
 * @author hanhan.zhang
 * */
public interface FeatureRel extends RelNode {

    Convention CONVENTION = new Convention.Impl("Feature", FeatureRel.class);

    CompletableFuture<FeatureData> convertTo(FeatureContext ctx);

}
