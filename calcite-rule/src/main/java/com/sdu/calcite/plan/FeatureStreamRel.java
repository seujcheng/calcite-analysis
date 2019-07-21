package com.sdu.calcite.plan;

import com.sdu.calcite.feature.FeatureStream;
import org.apache.calcite.rel.RelNode;

/**
 * @author hanhan.zhang
 * */
public interface FeatureStreamRel extends RelNode {

    FeatureStream convertTo();

}
