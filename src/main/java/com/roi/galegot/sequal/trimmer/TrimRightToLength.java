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
			if (sequence.isHasQual()) {
				sequence.setQualityString(sequence.getQualityString().substring(0, limit));
			}
		}

		return sequence;
	}
}
