package com.sdu.calcite.plan.rules;

import com.sdu.calcite.plan.FeatureRel;
import org.apache.calcite.plan.Convention;

/**
 * @author hanhan.zhang
 * */
public class FeatureConventions {

	public static Convention LOGICAL = new Convention.Impl("FEATURE_LOGICAL", FeatureRel.class);


}


