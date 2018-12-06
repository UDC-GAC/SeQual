package com.roi.galegot.sequal.trimmer;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class TrimNLeft.
 */
public class TrimNLeft implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -8433853203449156471L;

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

		limitStr = ExecutionParametersManager.getParameter("TrimNLeft");

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
			String strToCheck = sequence.getSequenceString().substring(0, limit);
			if (strToCheck.matches("N{" + limit + "}")) {
				sequence.setSequenceString(sequence.getSequenceString().substring(limit));
				if (sequence.isHasQual()) {
					sequence.setQualityString(sequence.getQualityString().substring(limit));
				}
			}
		}

		return sequence;
	}

}
