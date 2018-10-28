package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class GCP implements SingleFilter {

	private static final long serialVersionUID = 4373618581457549681L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Double limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("GCPMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("GCPMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Double(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return seqs;
		}
		return seqs.filter(s -> filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((seq.getGuaCytP() >= limMin) && (seq.getGuaCytP() <= limMax));
		}
		if (limMinUse) {
			return (seq.getGuaCytP() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getGuaCytP() <= limMax);
		}
		return true;
	}
}