package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

import scala.Tuple2;

/**
 * The Class AlmostDistinct.
 */
public class AlmostDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 3561681376850129189L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		String sDiff = ExecutionParametersManager.getParameter("MaxDifference");

		Integer maxDiff = (StringUtils.isNotBlank(sDiff)) ? new Integer(sDiff) : 0;

		JavaPairRDD<AlmostString, Sequence> group = sequences.mapToPair(
				seq -> new Tuple2<AlmostString, Sequence>(new AlmostString(seq.getSequenceString(), maxDiff), seq));

		if (sequences.first().isHasQual()) {
			return group.reduceByKey((seq1, seq2) -> {
				if (seq1.getQuality() >= seq2.getQuality()) {
					return seq1;
				}
				return seq2;
			}).values();
		} else {
			return group.reduceByKey((seq1, seq2) -> seq1).values();
		}
	}

	/**
	 * The Class AlmostString.
	 */
	class AlmostString implements Serializable {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 3595254670141051507L;

		/** The sequence. */
		private String sequence;

		/** The max diff. */
		private int maxDiff;

		/**
		 * Instantiates a new almost string.
		 *
		 * @param sequence the sequence
		 * @param maxDiff  the max diff
		 */
		public AlmostString(String sequence, int maxDiff) {
			this.sequence = sequence;
			this.maxDiff = maxDiff;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object obj) {
			AlmostString other = (AlmostString) obj;
			if (!this.checkSeq(other.sequence)) {
				return false;
			}
			return true;
		}

		/**
		 * Check seq.
		 *
		 * @param s2 the s 2
		 * @return the boolean
		 */
		private Boolean checkSeq(String s2) {
			if (this.sequence.length() != s2.length()) {
				return false;
			}

			if ((this.maxDiff > 0) && (this.maxDiff < this.sequence.length())) {
				int counter = 0;
				int diffs = 0;
				for (char c : this.sequence.toCharArray()) {
					if (c != s2.charAt(counter)) {
						diffs++;
					}
					if (diffs > this.maxDiff) {
						return false;
					}
					counter++;
				}
				return true;
			} else {
				return this.sequence.equals(s2);
			}
		}

	}
}