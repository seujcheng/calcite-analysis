package com.sdu.calcite.feature.fetcher;

import com.sdu.calcite.feature.FeatureRequest;
import com.sdu.calcite.feature.FeatureRow;
import com.sdu.calcite.utils.Tuple2;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author hanhan.zhang
 * */
public interface FeatureGetter {

	CompletableFuture<Tuple2<String, List<FeatureRow>>> getFeatures(FeatureRequest request);


}
