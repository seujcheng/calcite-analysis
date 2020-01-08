package com.sdu.calcite.utils;

import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class MapUtils {

	public static boolean isNullOrEmpty(Map<?, ?> m) {
		return m == null || m.isEmpty();
	}

	public static boolean isNotEmpty(Map<?, ?> m) {
		return !isNullOrEmpty(m);
	}

}
