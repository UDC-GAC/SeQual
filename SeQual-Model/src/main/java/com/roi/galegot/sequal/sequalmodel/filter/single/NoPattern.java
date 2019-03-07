package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class NoPattern.
 */
public class NoPattern implements SingleFilter {

	private static final long serialVersionUID = -661249387275097054L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		String pattern;
		String repsStr;
		String fullPattern;
		String finalPattern;

		Integer reps;

		if (sequences.isEmpty()) {
			return sequences;
		}

		pattern = ExecutionParametersManager.getParameter(FilterParametersNaming.NO_PATTERN);
		repsStr = ExecutionParametersManager.getParameter(FilterParametersNaming.REP_NO_PATTERN);

		if (StringUtils.isBlank(pattern) || StringUtils.isBlank(repsStr)) {
			return sequences;
		}

		reps = new Integer(repsStr);
		fullPattern = "";
		if (reps > 999) {
			throw new RuntimeException("RepNoPattern must be 999 or less.");
		}

		for (int i = 0; i < reps; i++) {
			fullPattern = fullPattern + pattern;
		}

		if (fullPattern.isEmpty()) {
			return sequences;
		}

		finalPattern = fullPattern;

		if (sequences.first().getIsPaired()) {
			return sequences.filter(s -> this.filter(s, finalPattern) && this.filterPair(s, finalPattern));
		}

		return sequences.filter(s -> this.filter(s, finalPattern));
	}

	/**
	 * Filter.
	 *
	 * @param sequence     the sequence
	 * @param finalPattern the final pattern
	 * @return the boolean
	 */
	private Boolean filter(Sequence sequence, String finalPattern) {
		return this.compare(sequence.getSequenceString(), finalPattern);
	}

	/**
	 * Filter pair.
	 *
	 * @param sequence     the sequence
	 * @param finalPattern the final pattern
	 * @return the boolean
	 */
	private Boolean filterPair(Sequence sequence, String finalPattern) {
		return this.compare(sequence.getSequenceStringPair(), finalPattern);
	}

	/**
	 * Compare.
	 *
	 * @param sequenceString the sequence string
	 * @param finalPattern   the final pattern
	 * @return the boolean
	 */
	private Boolean compare(String sequenceString, String finalPattern) {
		return !sequenceString.contains(finalPattern);
	}
}