package com.sdu.calcite.feature;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.ArrayUtils;

/**
 * @author hanhan.zhang
 * */
public class FeatureRow {

	private final String[] names;
	private final Object[] values;

	private FeatureRow(String[] names, Object[] values) {
		Preconditions.checkArgument(!ArrayUtils.isEmpty(names));
		Preconditions.checkArgument(!ArrayUtils.isEmpty(values));
		Preconditions.checkState(names.length == values.length);

		this.names = names;
		this.values = values;
	}


	public static FeatureRow of(String[] names, Object[] values) {
		return new FeatureRow(names, values);
	}
}
