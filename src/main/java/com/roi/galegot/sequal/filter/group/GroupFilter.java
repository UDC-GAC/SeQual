package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public interface GroupFilter extends Serializable {

	/**
	 * Filters a JavaRDD of Sequence and returns the result
	 *
	 * @param seqs RDD to be filtered
	 * @return JavaRDD<Sequence> of remaining sequences
	 */
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs);

}