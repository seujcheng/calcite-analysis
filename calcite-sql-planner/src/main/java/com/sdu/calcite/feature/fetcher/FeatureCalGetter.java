package com.sdu.calcite.feature.fetcher;

import com.google.common.base.Preconditions;
import com.sdu.calcite.feature.FeatureRequest;
import com.sdu.calcite.feature.FeatureRow;
import com.sdu.calcite.utils.Tuple2;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class FeatureCalGetter implements FeatureGetter {

	private final FeatureGetter input;

	private FeatureCalGetter(FeatureGetter input) {
		Preconditions.checkArgument(input != null);
		this.input = input;
	}

	@Override
	public CompletableFuture<Tuple2<String, List<FeatureRow>>> getFeatures(FeatureRequest request) {
		CompletableFuture<Tuple2<String, List<FeatureRow>>> inputFeatureRows = input.getFeatures(request);
		return inputFeatureRows.thenApply(new FeatureCalFunction());
	}

	private static class FeatureCalFunction implements Function<Tuple2<String, List<FeatureRow>>, Tuple2<String, List<FeatureRow>>> {

		private FeatureCalFunction() {

		}

		@Override
		public Tuple2<String, List<FeatureRow>> apply(Tuple2<String, List<FeatureRow>> inputFeatureRows) {
			// TODO: 2019-08-09 处理逻辑
			return null;
		}
	}


	public static FeatureCalGetter of(FeatureGetter input) {
		return new FeatureCalGetter(input);
	}
}
