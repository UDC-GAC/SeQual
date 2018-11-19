package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.roi.galegot.sequal.common.Sequence;

import scala.Tuple2;

/**
 * The Class ReverseComplementDistinct.
 */
public class ReverseComplementDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -3416801873476204891L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		JavaPairRDD<ReverseComplementString, Sequence> group = sequences
				.mapToPair(new PairFunction<Sequence, ReverseComplementString, Sequence>() {

					private static final long serialVersionUID = -6192877628808422227L;

					@Override
					public Tuple2<ReverseComplementString, Sequence> call(Sequence seq) {
						return new Tuple2<ReverseComplementString, Sequence>(
								new ReverseComplementString(seq.getSequenceString()), seq);
					}
				});

		if (sequences.first().isHasQual()) {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = -2285412703072618941L;

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

				private static final long serialVersionUID = -6498038456070131065L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					return seq1;
				}
			}).values();
		}
	}

	/**
	 * The Class ReverseComplementString.
	 */
	class ReverseComplementString implements Serializable {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = -7032196466156045735L;

		/** The sequence. */
		private String sequence;

		/** The reverse complementary sequence. */
		private String reverseComplementarySequence;

		/**
		 * Instantiates a new reverse complement string.
		 *
		 * @param sequence the sequence
		 */
		public ReverseComplementString(String sequence) {
			this.sequence = sequence;
			this.reverseComplementarySequence = new StringBuilder(this.getComplementary(sequence)).reverse().toString();
		}

		/**
		 * Gets the complementary.
		 *
		 * @param sequence the sequence
		 * @return the complementary
		 */
		private String getComplementary(String sequence) {
			char[] result = new char[sequence.length()];
			int counter = 0;
			for (char c : this.sequence.toCharArray()) {

				switch (c) {
				case 'A':
					result[counter] = 'T';
					break;
				case 'G':
					result[counter] = 'C';
					break;
				case 'T':
					result[counter] = 'A';
					break;
				case 'C':
					result[counter] = 'G';
					break;
				default:
					result[counter] = c;
				}
				counter++;
			}
			return String.valueOf(result);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.reverseComplementarySequence == null) ? 0 : this.reverseComplementarySequence.hashCode()));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			ReverseComplementString other = (ReverseComplementString) obj;
			if (this.sequence == null) {
				if (other.reverseComplementarySequence != null) {
					return false;
				}
			} else if (!this.sequence.equals(other.reverseComplementarySequence)) {
				return false;
			}
			if (this.reverseComplementarySequence == null) {
				if (other.sequence != null) {
					return false;
				}
			} else if (!this.reverseComplementarySequence.equals(other.sequence)) {
				return false;
			}
			return true;
		}
	}

}