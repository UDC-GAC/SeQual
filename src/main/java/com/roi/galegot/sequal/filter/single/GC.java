package com.roi.galegot.sequal.filter.single;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class GC implements SingleFilter {

	private static final long serialVersionUID = 8628696067644155529L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		Integer limMin, limMax;
		String limMinStr, limMaxStr;
		Boolean limMinUse, limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter("GCMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("GCMaxVal");

		limMin = (limMinUse = !(limMinStr.isEmpty())) ? new Integer(limMinStr) : null;
		limMax = (limMaxUse = !(limMaxStr.isEmpty())) ? new Integer(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return seqs;
		}
		return seqs.filter(s -> filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	private Boolean filter(Sequence seq, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((seq.getGuaCyt() >= limMin) && (seq.getGuaCyt() <= limMax));
		}
		if (limMinUse) {
			return (seq.getGuaCyt() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getGuaCyt() <= limMax);
		}
		return true;
	}
}