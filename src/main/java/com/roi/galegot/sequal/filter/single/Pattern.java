package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class Pattern.
 */
public class Pattern implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5371376391297589941L;

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

		pattern = ExecutionParametersManager.getParameter("Pattern");
		repsStr = ExecutionParametersManager.getParameter("RepPattern");

		if (StringUtils.isBlank(pattern) || StringUtils.isBlank(repsStr)) {
			return sequences;
		}

		reps = new Integer(repsStr);
		fullPattern = "";
		if (reps > 999) {
			throw new RuntimeException("RepPattern must be 999 or less.");
		}

		for (int i = 0; i < reps; i++) {
			fullPattern = fullPattern + pattern;
		}

		if (fullPattern.isEmpty()) {
			return sequences;
		}

		finalPattern = fullPattern;

		return sequences.filter(s -> this.filter(s, finalPattern));
	}

	/**
	 * Filter.
	 *
	 * @param seq          the seq
	 * @param finalPattern the final pattern
	 * @return the boolean
	 */
	private Boolean filter(Sequence seq, String finalPattern) {
		return seq.getSequenceString().contains(finalPattern);
	}
}