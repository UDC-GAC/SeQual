package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class Pattern implements SingleFilter {

	private static final long serialVersionUID = 5371376391297589941L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		String pattern, repsStr, fullPattern, finalPattern;
		Integer reps;

		pattern = ExecutionParametersManager.getParameter("Pattern");
		repsStr = ExecutionParametersManager.getParameter("RepPattern");

		if (pattern.isEmpty() || repsStr.isEmpty()) {
			return seqs;
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
			return seqs;
		}

		finalPattern = fullPattern;

		return seqs.filter(s -> filter(s, finalPattern));
	}

	private Boolean filter(Sequence seq, String finalPattern) {
		return seq.getSeq().contains(finalPattern);
	}
}