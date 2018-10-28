package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class QualityScore implements SingleFilter {

	private static final long serialVersionUID = 1986175935593684878L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Double limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("QualityScoreMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("QualityScoreMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Double(limMaxStr) : null;

		if ((!limMinUse && !limMaxUse) || !seqs.first().isHasQual()) {
			return seqs;
		}
		return seqs.filter(s -> this.filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (seq.isHasQual()) {
			if (limMinUse && limMaxUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if ((qual > limMax) || (qual < limMin)) {
						return false;
					}
				}
			}
			if (limMinUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if (qual < limMin) {
						return false;
					}
				}
			}
			if (limMaxUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if (qual > limMax) {
						return false;
					}
				}
			}
			return true;
		} else {
			return false;
		}
	}

}