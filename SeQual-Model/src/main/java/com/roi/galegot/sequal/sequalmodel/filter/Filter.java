package com.roi.galegot.sequal.sequalmodel.filter;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

public interface Filter extends Serializable {

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences);

}
