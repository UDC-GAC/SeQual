package com.roi.galegot.sequal.sequalmodel.stat;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

public interface Stat extends Serializable {

	/**
	 * Measure.
	 *
	 * @param seqs the seqs
	 * @return the double
	 */
	public Double measure(JavaRDD<Sequence> seqs);

	/**
	 * Measure pair.
	 *
	 * @param seqs the seqs
	 * @return the double
	 */
	public Double measurePair(JavaRDD<Sequence> seqs);

}