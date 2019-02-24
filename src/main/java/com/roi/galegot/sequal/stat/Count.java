package com.roi.galegot.sequal.stat;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

/**
 * The Class Count.
 */
public class Count implements Stat {

	private static final long serialVersionUID = -8997982132559107882L;

	/**
	 * Measure.
	 *
	 * @param sequences the sequences
	 * @return the double
	 */
	@Override
	public Double measure(JavaRDD<Sequence> sequences) {
		Long count = sequences.count();

		return count.doubleValue();
	}

	/**
	 * Measure pair.
	 *
	 * @param sequences the sequences
	 * @return the double
	 */
	@Override
	public Double measurePair(JavaRDD<Sequence> sequences) {
		return this.measure(sequences);
	}

}