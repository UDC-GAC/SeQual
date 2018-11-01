package com.roi.galegot.sequal.filter.group;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

import scala.Tuple2;

/**
 * The Class Distinct.
 */
public class Distinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -4086415405257136932L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		if (sequences.first().isHasQual()) {
			JavaPairRDD<String, Sequence> group = sequences
					.mapToPair(seq -> new Tuple2<String, Sequence>(seq.getSequenceString(), seq));
			return group.reduceByKey((seq1, seq2) -> {
				if (seq1.getQuality() >= seq2.getQuality()) {
					return seq1;
				}
				return seq2;
			}).values();
		} else {
			return sequences.distinct();
		}
	}

}