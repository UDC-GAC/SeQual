package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.roi.galegot.sequal.common.Sequence;

import scala.Tuple2;

/**
 * The Class ComplementDistinct.
 */
public class ComplementDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5947189310641527595L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		JavaPairRDD<ComplementString, Sequence> group = sequences
				.mapToPair(new PairFunction<Sequence, ComplementString, Sequence>() {

					private static final long serialVersionUID = -7740337890433733128L;

					@Override
					public Tuple2<ComplementString, Sequence> call(Sequence seq) {
						return new Tuple2<ComplementString, Sequence>(new ComplementString(seq.getSequenceString()),
								seq);
					}
				});

		if (sequences.first().isHasQual()) {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = 409867625719430118L;

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

				private static final long serialVersionUID = 6231420965110817797L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					return seq1;
				}
			}).values();
		}
	}

	/**
	 * The Class ComplementString.
	 */
	class ComplementString implements Serializable {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 3011027347808993546L;

		/** The complementary sequence. */
		private String sequence, complementarySequence;

		/**
		 * Instantiates a new complement string.
		 *
		 * @param sequence the sequence
		 */
		public ComplementString(String sequence) {
			this.sequence = sequence;
			this.complementarySequence = this.getComplementary(sequence);
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
					+ ((this.complementarySequence == null) ? 0 : this.complementarySequence.hashCode()));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ComplementString other = (ComplementString) obj;
			if (!this.sequence.equals(other.complementarySequence)) {
				return false;
			}
			return true;
		}
	}

}