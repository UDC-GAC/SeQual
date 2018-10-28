package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class Length implements SingleFilter {

	private static final long serialVersionUID = 8350107142175010716L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Integer limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("LengthMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("LengthMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Integer(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Integer(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return seqs;
		}
		return seqs.filter(s -> filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((seq.getLength() >= limMin) && (seq.getLength() <= limMax));
		}
		if (limMinUse) {
			return (seq.getLength() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getLength() <= limMax);
		}
		return true;
	}
}