package com.roi.galegot.sequal.stat;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public interface Stat extends Serializable {

	public Double measure(JavaRDD<Sequence> seqs);

}