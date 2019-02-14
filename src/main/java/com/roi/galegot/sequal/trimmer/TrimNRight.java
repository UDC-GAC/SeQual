package com.roi.galegot.sequal.trimmer;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class TrimNRight.
 */
public class TrimNRight implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1373653504736559043L;

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

		limitStr = ExecutionParametersManager.getParameter("TrimNRight");

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
		int length = sequence.getLength();
		if (length > limit) {
			String strToCheck = sequence.getSequenceString().substring(length - limit);
			if (strToCheck.matches("N{" + limit + "}")) {
				sequence.setSequenceString(sequence.getSequenceString().substring(0, length - limit));
				if (sequence.getHasQuality()) {
					sequence.setQualityString(sequence.getQualityString().substring(0, length - limit));
				}
			}
		}

		return sequence;
	}

	private Sequence doTrimPair(Sequence sequence, Integer limit) {

		// TODO

		int length = sequence.getLength();
		if (length > limit) {
			String strToCheck = sequence.getSequenceString().substring(length - limit);
			if (strToCheck.matches("N{" + limit + "}")) {
				sequence.setSequenceString(sequence.getSequenceString().substring(0, length - limit));
				if (sequence.getHasQuality()) {
					sequence.setQualityString(sequence.getQualityString().substring(0, length - limit));
				}
			}
		}

		return sequence;
	}

}
