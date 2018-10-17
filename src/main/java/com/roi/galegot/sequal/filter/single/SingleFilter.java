package com.roi.galegot.sequal.filter.single;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public interface SingleFilter extends Serializable {

	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs);

}