package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class GCP.
 */
public class GCP implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 4373618581457549681L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		Double limMin;
		Double limMax;

		String limMinStr;
		String limMaxStr;

		Boolean limMinUse;
		Boolean limMaxUse;

		if (sequences.isEmpty()) {
			return sequences;
		}

		limMinStr = ExecutionParametersManager.getParameter("GCPMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("GCPMaxVal");

		limMin = (limMinUse = StringUtils.isNotBlank(limMinStr)) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = StringUtils.isNotBlank(limMaxStr)) ? new Double(limMaxStr) : null;

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

	private Boolean filterPair(Sequence seq, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {

		// TODO

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