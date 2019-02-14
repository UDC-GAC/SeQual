package com.roi.galegot.sequal.trimmer;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class TrimLeftP.
 */
public class TrimLeftP implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 3195898863006276645L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		String percentageStr;
		Double percentage;

		if (sequences.isEmpty()) {
			return sequences;
		}

		percentageStr = ExecutionParametersManager.getParameter("TrimLeftP");

		if (StringUtils.isBlank(percentageStr)) {
			return sequences;
		}

		percentage = new Double(percentageStr);

		if ((percentage <= 0) || (percentage >= 1)) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.map(sequence -> this.doTrimPair(sequence, percentage));
		}

		return sequences.map(sequence -> this.doTrim(sequence, percentage));
	}

	/**
	 * Do trim.
	 *
	 * @param sequence   the sequence
	 * @param percentage the percentage
	 * @return the sequence
	 */
	private Sequence doTrim(Sequence sequence, Double percentage) {
		Integer valueToTrim = (int) (percentage * sequence.getLength());
		sequence.setSequenceString(sequence.getSequenceString().substring(valueToTrim));
		if (sequence.getHasQuality()) {
			sequence.setQualityString(sequence.getQualityString().substring(valueToTrim));
		}

		return sequence;
	}

	private Sequence doTrimPair(Sequence sequence, Double percentage) {

		// TODO

		Integer valueToTrim = (int) (percentage * sequence.getLength());
		sequence.setSequenceString(sequence.getSequenceString().substring(valueToTrim));
		if (sequence.getHasQuality()) {
			sequence.setQualityString(sequence.getQualityString().substring(valueToTrim));
		}

		return sequence;
	}

}