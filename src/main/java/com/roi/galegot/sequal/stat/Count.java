package com.roi.galegot.sequal.stat;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

/**
 * The Class Count.
 */
public class Count implements Stat {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -8997982132559107882L;

	/**
	 * Measure.
	 *
	 * @param seqs the seqs
	 * @return the double
	 */
	@Override
	public Double measure(JavaRDD<Sequence> seqs) {
		Long count = seqs.count();

		return count.doubleValue();
	}

}