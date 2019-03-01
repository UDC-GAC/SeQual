package com.roi.galegot.sequal.sequalmodel.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.common.SequenceUtils;

import scala.Tuple2;

/**
 * The Class Distinct.
 */
public class Distinct implements GroupFilter {

	private static final long serialVersionUID = -4086415405257136932L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		return this.filter(sequences);
	}

	/**
	 * Filter.
	 *
	 * @param sequences     the sequences
	 * @param maxDifference the max difference
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filter(JavaRDD<Sequence> sequences) {

		if (!sequences.first().getHasQuality()) {
			return sequences.distinct();
		}

		if (sequences.first().getIsPaired()) {
			return this.filterPairedEnd(sequences);
		}

		return this.filterSingleEnd(sequences);

	}

	/**
	 * Filter single.
	 *
	 * @param sequences     the sequences
	 * @param maxDifference the max difference
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterSingleEnd(JavaRDD<Sequence> sequences) {
		JavaPairRDD<String, Sequence> group = sequences
				.mapToPair(seq -> new Tuple2<String, Sequence>(seq.getSequenceString(), seq));

		return group.reduceByKey((seq1, seq2) -> SequenceUtils.selectSequenceWithMaxQuality(seq1, seq2)).values();
	}

	/**
	 * Filter pair.
	 *
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterPairedEnd(JavaRDD<Sequence> sequences) {
		JavaPairRDD<SequenceWithPairString, Sequence> group = sequences
				.mapToPair(sequence -> new Tuple2<SequenceWithPairString, Sequence>(
						new SequenceWithPairString(sequence.getSequenceString(), sequence.getSequenceStringPair()),
						sequence));

		return group.reduceByKey((seq1, seq2) -> SequenceUtils.selectSequenceWithMaxQuality(seq1, seq2)).values();
	}

	/**
	 * The Class SequenceWithPairString.
	 */
	class SequenceWithPairString implements Serializable {

		private static final long serialVersionUID = 3595254670141051507L;

		protected String sequence;
		protected String sequencePair;

		/**
		 * Instantiates a new sequence with pair string.
		 *
		 * @param sequence     the sequence
		 * @param sequencePair the sequence pair
		 */
		public SequenceWithPairString(String sequence, String sequencePair) {
			this.sequence = sequence;
			this.sequencePair = sequencePair;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object obj) {
			SequenceWithPairString other = (SequenceWithPairString) obj;

			return this.sequence.equals(other.sequence) && this.sequencePair.equals(other.sequencePair);
		}
	}

}