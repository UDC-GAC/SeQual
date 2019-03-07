package com.roi.galegot.sequal.sequalmodel.trimmer;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class TrimQualRight.
 */
public class TrimQualRight implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -2523685770552633194L;

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

		if (sequences.isEmpty() || !sequences.first().getHasQuality()) {
			return sequences;
		}

		limitStr = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIM_QUAL_RIGHT);

		if (StringUtils.isBlank(limitStr)) {
			return sequences;
		}

		limit = new Double(limitStr);

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
	private Sequence doTrim(Sequence sequence, Double limit) {
		while ((sequence.getQuality() > limit) && (sequence.getQualityString().length() > 1)) {
			int length = sequence.getLength();
			sequence.setSequenceString(sequence.getSequenceString().substring(0, length - 1));
			sequence.setQualityString(sequence.getQualityString().substring(0, length - 1));
		}
		return sequence;
	}

	private Sequence doTrimPair(Sequence sequence, Double limit) {

		this.doTrim(sequence, limit);

		while ((sequence.getQualityPair() > limit) && (sequence.getQualityStringPair().length() > 1)) {
			int length = sequence.getLengthPair();
			sequence.setSequenceStringPair(sequence.getSequenceStringPair().substring(0, length - 1));
			sequence.setQualityStringPair(sequence.getQualityStringPair().substring(0, length - 1));
		}

		return sequence;
	}

}