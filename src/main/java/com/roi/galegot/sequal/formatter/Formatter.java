package com.roi.galegot.sequal.formatter;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public interface Formatter extends Serializable {

	/**
	 * Format.
	 *
	 * @param seqs the seqs
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> format(JavaRDD<Sequence> seqs);

}