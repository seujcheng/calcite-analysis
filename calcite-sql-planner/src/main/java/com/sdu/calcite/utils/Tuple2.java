package com.sdu.calcite.utils;

import java.io.Serializable;

public class Tuple2<T1, T2> implements Serializable {

	private final T1 f0;
	private final T2 f2;

	public Tuple2(T1 f0, T2 f2) {
		this.f0 = f0;
		this.f2 = f2;
	}

	public T1 f0() {
		return f0;
	}

	public T2 f1() {
		return f2;
	}

}
