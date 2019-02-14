package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.roi.galegot.sequal.common.Sequence;

import scala.Tuple2;

/**
 * The Class ReverseDistinct.
 */
public class ReverseDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1061447508688116490L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		JavaPairRDD<ReverseString, Sequence> group = sequences
				.mapToPair(new PairFunction<Sequence, ReverseString, Sequence>() {

					private static final long serialVersionUID = -3184033798508472514L;

					@Override
					public Tuple2<ReverseString, Sequence> call(Sequence seq) {
						return new Tuple2<ReverseString, Sequence>(new ReverseString(seq.getSequenceString()), seq);
					}
				});

		if (sequences.first().getHasQuality()) {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = -1935325326481753717L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					if (seq1.getQuality() >= seq2.getQuality()) {
						return seq1;
					}
					return seq2;
				}
			}).values();
		} else {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = -3885295642075191150L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					return seq1;
				}
			}).values();
		}
	}

	/**
	 * The Class ReverseString.
	 */
	class ReverseString implements Serializable {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = -3467393373228813336L;

		/** The reverse sequence. */
		private String sequence, reverseSequence;

		/**
		 * Instantiates a new reverse string.
		 *
		 * @param sequence the sequence
		 */
		public ReverseString(String sequence) {
			this.sequence = sequence;
			this.reverseSequence = new StringBuilder(sequence).reverse().toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.reverseSequence == null) ? 0 : this.reverseSequence.hashCode()));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ReverseString other = (ReverseString) obj;
			if (!this.sequence.equals(other.reverseSequence)) {
				return false;
			}
			return true;
		}

	}

}