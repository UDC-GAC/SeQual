package com.roi.galegot.sequal.filter.group;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import scala.Tuple2;

public class Distinct implements GroupFilter {

	private static final long serialVersionUID = -4086415405257136932L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		if (seqs.first().isHasQual()) {
			JavaPairRDD<String, Sequence> group = seqs
					.mapToPair(seq -> new Tuple2<String, Sequence>(seq.getSeq(), seq));
			return group.reduceByKey((seq1, seq2) -> {
				if (seq1.getQuality() >= seq2.getQuality()) {
					return seq1;
				}
				return seq2;
			}).values();
		} else {
			return seqs.distinct();
		}
	}

}