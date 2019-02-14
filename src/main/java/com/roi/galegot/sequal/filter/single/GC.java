package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class GC.
 */
public class GC implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 8628696067644155529L;

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

		limMinStr = ExecutionParametersManager.getParameter("GCMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("GCMaxVal");

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

	private Boolean filterPair(Sequence seq, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {

		// TODO

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