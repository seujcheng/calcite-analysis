package com.sdu.calcite.plan.nodes;

import com.sdu.calcite.plan.FeatureStreamRel;
import org.apache.calcite.plan.Convention;

public class Conventions {

    public static Convention FEATURESTREAM = new Convention.Impl("FEATURESTREAM", FeatureStreamRel.class);

    private Conventions() {}



}
