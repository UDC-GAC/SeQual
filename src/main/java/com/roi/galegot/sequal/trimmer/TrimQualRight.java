package com.roi.galegot.sequal.trimmer;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class TrimQualRight.
 */
public class TrimQualRight implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 8938634994260655125L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		String limitStr;
		Double limit;

		if (sequences.isEmpty() || !sequences.first().isHasQual()) {
			return sequences;
		}

		limitStr = ExecutionParametersManager.getParameter("TrimQualRight");

		if (StringUtils.isBlank(limitStr)) {
			return sequences;
		}

		limit = new Double(limitStr);

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
	private Sequence doTrim(Sequence sequence, Double limit) {
		while ((sequence.getQuality() > limit) && (sequence.getQualityString().length() > 1)) {
			int length = sequence.getLength();
			sequence.setSequenceString(sequence.getSequenceString().substring(0, length - 1));
			sequence.setQualityString(sequence.getQualityString().substring(0, length - 1));
		}
		return sequence;
	}

}