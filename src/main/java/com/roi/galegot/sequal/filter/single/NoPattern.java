package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class NoPattern.
 */
public class NoPattern implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -661249387275097054L;

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

		pattern = ExecutionParametersManager.getParameter("NoPattern");
		repsStr = ExecutionParametersManager.getParameter("RepNoPattern");

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
			return sequences.filter(s -> this.filterPair(s, finalPattern));
		}

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
		return !seq.getSequenceString().contains(finalPattern);
	}

	private Boolean filterPair(Sequence seq, String finalPattern) {

		// TODO

		return !seq.getSequenceString().contains(finalPattern);
	}
}