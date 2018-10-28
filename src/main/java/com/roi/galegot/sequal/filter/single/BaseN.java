package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class BaseN implements SingleFilter {

	private static final long serialVersionUID = -114406829239103678L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		String[] bases, baseMin, baseMax;
		String basesMinStr, basesMaxStr;
		Boolean limMaxUse, limMinUse;

		String basesStr = ExecutionParametersManager.getParameter("Base");
		if (basesStr.isEmpty()) {
			return seqs;
		} else {
			bases = basesStr.split("\\|");
		}

		basesMinStr = ExecutionParametersManager.getParameter("BaseMinVal");
		if (basesMinStr.isEmpty()) {
			baseMin = null;
			limMinUse = false;
		} else {
			baseMin = basesMinStr.split("\\|");
			limMinUse = true;
		}

		basesMaxStr = ExecutionParametersManager.getParameter("BaseMaxVal");
		if (basesMaxStr.isEmpty()) {
			baseMax = null;
			limMaxUse = false;
		} else {
			baseMax = basesMaxStr.split("\\|");
			limMaxUse = true;
		}

		if (!limMinUse && !limMaxUse) {
			return seqs;
		}

		if ((limMinUse && (bases.length != baseMin.length)) || (limMaxUse && (bases.length != baseMax.length))) {
			throw new RuntimeException("Incorrect number of parameters");
		}

		return seqs.filter(s -> filter(s, bases, baseMin, limMinUse, baseMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, String[] bases, String[] baseMin, Boolean limMinUse, String[] baseMax,
			Boolean limMaxUse) {
		Integer lim1, lim2;
		for (int i = 0; i < bases.length; i++) {
			int reps = StringUtils.countMatches(seq.getSeq(), bases[i]);
			if (limMinUse) {
				lim1 = new Integer(baseMin[i]);
				if ((lim1 != -1) && (reps < lim1)) {
					return false;
				}
			}
			if (limMaxUse) {
				lim2 = new Integer(baseMax[i]);
				if ((lim2 != -1) && (reps > lim2)) {
					return false;
				}
			}
		}
		return true;
	}
}