package com.sdu.calcite.utils;

import java.util.Collection;

/**
 * @author hanhan.zhang
 * */
public class CollectionUtils {

	private CollectionUtils() {}

	public static boolean isNullOrEmpty(Collection<?> c) {
		return c == null || c.isEmpty();
	}

	public static boolean isNotEmpty(Collection<?> c) {
		return !isNullOrEmpty(c);
	}

}
