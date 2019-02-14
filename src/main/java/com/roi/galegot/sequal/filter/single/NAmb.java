package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class NAmb.
 */
public class NAmb implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -7389192873369227802L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		Integer limMin;
		Integer limMax;

		String limMinStr;
		String limMaxStr;

		Boolean limMinUse;
		Boolean limMaxUse;

		if (sequences.isEmpty()) {
			return sequences;
		}

		limMinStr = ExecutionParametersManager.getParameter("NAmbMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("NAmbMaxVal");

		limMin = (limMinUse = StringUtils.isNotBlank(limMinStr)) ? new Integer(limMinStr) : null;
		limMax = (limMaxUse = StringUtils.isNotBlank(limMaxStr)) ? new Integer(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.filter(s -> this.filterPair(s, limMin, limMinUse, limMax, limMaxUse));
		}

		return sequences.filter(s -> this.filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	/**
	 * Filter.
	 *
	 * @param seq       the seq
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filter(Sequence seq, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((seq.getnAmb() >= limMin) && (seq.getnAmb() <= limMax));
		}
		if (limMinUse) {
			return (seq.getnAmb() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getnAmb() <= limMax);
		}
		return true;
	}

	private Boolean filterPair(Sequence seq, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {

		// TODO

		if (limMinUse && limMaxUse) {
			return ((seq.getnAmb() >= limMin) && (seq.getnAmb() <= limMax));
		}
		if (limMinUse) {
			return (seq.getnAmb() >= limMin);
		}
		if (limMaxUse) {
			return (seq.getnAmb() <= limMax);
		}
		return true;
	}
}