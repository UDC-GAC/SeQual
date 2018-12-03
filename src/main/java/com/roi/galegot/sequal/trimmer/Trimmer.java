package com.roi.galegot.sequal.trimmer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

/**
 * The Interface Trimmer.
 */
public interface Trimmer extends Serializable {

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences);

}