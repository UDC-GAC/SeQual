package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class NAmbP implements SingleFilter {

	private static final long serialVersionUID = -390276120695449143L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Double limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("NAmbPMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("NAmbPMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Double(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return seqs;
		}
		return seqs.filter(s -> filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((seq.getNAmbP() >= limMin) && (seq.getNAmbP() <= limMax));
		}
		if (limMinUse) {
			return (seq.getNAmbP() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getNAmbP() <= limMax);
		}
		return true;
	}
}