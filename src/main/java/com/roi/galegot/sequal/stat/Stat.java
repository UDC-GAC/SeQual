package com.roi.galegot.sequal.stat;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public interface Stat extends Serializable {

	/**
	 * Measure.
	 *
	 * @param seqs the seqs
	 * @return the double
	 */
	public Double measure(JavaRDD<Sequence> seqs);

}