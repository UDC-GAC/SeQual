package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class NAmb implements SingleFilter {

	private static final long serialVersionUID = -7389192873369227802L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Integer limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("NAmbMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("NAmbMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Integer(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Integer(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return seqs;
		}
		return seqs.filter(s -> filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((seq.getNAmb() >= limMin) && (seq.getNAmb() <= limMax));
		}
		if (limMinUse) {
			return (seq.getNAmb() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getNAmb() <= limMax);
		}
		return true;
	}
}