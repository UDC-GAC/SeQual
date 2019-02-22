package com.roi.galegot.sequal.trimmer;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class TrimRightToLength.
 */
public class TrimRightToLength implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1953745377281159367L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		String limitStr;
		Integer limit;

		if (sequences.isEmpty()) {
			return sequences;
		}

		limitStr = ExecutionParametersManager.getParameter("TrimRightToLength");

		if (StringUtils.isBlank(limitStr)) {
			return sequences;
		}

		limit = new Integer(limitStr);

		if (limit <= 0) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.map(sequence -> this.doTrimPair(sequence, limit));
		}

		return sequences.map(sequence -> this.doTrim(sequence, limit));
	}

	/**
	 * Do trim.
	 *
	 * @param sequence the sequence
	 * @param limit    the limit
	 * @return the sequence
	 */
	private Sequence doTrim(Sequence sequence, Integer limit) {
		if (sequence.getLength() > limit) {
			sequence.setSequenceString(sequence.getSequenceString().substring(0, limit));
			if (sequence.getHasQuality()) {
				sequence.setQualityString(sequence.getQualityString().substring(0, limit));
			}
		}

		return sequence;
	}

	/**
	 * Do trim pair.
	 *
	 * @param sequence the sequence
	 * @param limit    the limit
	 * @return the sequence
	 */
	private Sequence doTrimPair(Sequence sequence, Integer limit) {

		this.doTrim(sequence, limit);

		if (sequence.getLengthPair() > limit) {
			sequence.setSequenceStringPair(sequence.getSequenceStringPair().substring(0, limit));
			if (sequence.getHasQuality()) {
				sequence.setQualityStringPair(sequence.getQualityStringPair().substring(0, limit));
			}
		}

		return sequence;
	}
}
