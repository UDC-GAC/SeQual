package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class BaseP implements SingleFilter {

	private static final long serialVersionUID = -3632770663351055062L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		String[] bases, baseMin, baseMax;
		String basesMinStr, basesMaxStr;
		Boolean limMaxUse, limMinUse;

		String basesStr = ExecutionParametersManager.getParameter("BaseP");
		if (basesStr.isEmpty()) {
			return seqs;
		} else {
			bases = basesStr.split("\\|");
		}

		basesMinStr = ExecutionParametersManager.getParameter("BasePMinVal");
		if (basesMinStr.isEmpty()) {
			baseMin = null;
			limMinUse = false;
		} else {
			baseMin = basesMinStr.split("\\|");
			limMinUse = true;
		}

		basesMaxStr = ExecutionParametersManager.getParameter("BasePMaxVal");
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
		Double lim1, lim2;
		for (int i = 0; i < bases.length; i++) {
			Double perc = (double) StringUtils.countMatches(seq.getSeq(), bases[i]) / seq.getSeq().length();
			if (limMinUse) {
				lim1 = new Double(baseMin[i]);
				if ((lim1 != -1) && (perc < lim1)) {
					return false;
				}
			}
			if (limMaxUse) {
				lim2 = new Double(baseMax[i]);
				if ((lim2 != -1) && (perc > lim2)) {
					return false;
				}
			}
		}
		return true;
	}
}