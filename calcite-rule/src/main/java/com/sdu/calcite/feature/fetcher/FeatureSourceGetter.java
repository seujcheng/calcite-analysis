package com.sdu.calcite.feature.fetcher;

import com.google.common.base.Preconditions;
import com.sdu.calcite.feature.FeatureRequest;
import com.sdu.calcite.feature.FeatureRow;
import com.sdu.calcite.utils.Tuple2;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author hanhan.zhang
 * */
public class FeatureSourceGetter implements FeatureGetter {

	private final String domainName;

	private FeatureSourceGetter(String domainName) {
		Preconditions.checkArgument(StringUtils.isNotEmpty(domainName));

		this.domainName = domainName;
	}


	@Override
	public CompletableFuture<Tuple2<String, List<FeatureRow>>> getFeatures(FeatureRequest request) {
		return null;
	}


	public static FeatureGetter of(String domainName) {
		return new FeatureSourceGetter(domainName);
	}
}
