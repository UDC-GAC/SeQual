package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class Quality implements SingleFilter {

	private static final long serialVersionUID = 6867361108805219701L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Double limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("QualityMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("QualityMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Double(limMaxStr) : null;

		if ((!limMinUse && !limMaxUse) || !seqs.first().isHasQual()) {
			return seqs;
		}
		return seqs.filter(s -> filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (seq.isHasQual()) {
			if (limMinUse && limMaxUse) {
				return ((seq.getQuality() >= limMin) && (seq.getQuality() <= limMax));
			}
			if (limMinUse) {
				return (seq.getQuality() >= limMin);
			}
			if (limMaxUse) {
				return (seq.getQuality() <= limMax);
			}
			return true;
		} else {
			return false;
		}
	}
}