package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class BaseP.
 */
public class BaseP implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -3632770663351055062L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		String[] bases;
		String[] baseMin;
		String[] baseMax;

		String basesMinStr;
		String basesMaxStr;

		Boolean limMaxUse;
		Boolean limMinUse;

		if (sequences.isEmpty()) {
			return sequences;
		}

		String basesStr = ExecutionParametersManager.getParameter("BaseP");
		if (StringUtils.isNotBlank(basesStr)) {
			bases = basesStr.split("\\|");
		} else {
			return sequences;
		}

		basesMinStr = ExecutionParametersManager.getParameter("BasePMinVal");
		if (StringUtils.isNotBlank(basesMinStr)) {
			baseMin = basesMinStr.split("\\|");
			limMinUse = true;
		} else {
			baseMin = null;
			limMinUse = false;
		}

		basesMaxStr = ExecutionParametersManager.getParameter("BasePMaxVal");
		if (StringUtils.isNotBlank(basesMaxStr)) {
			baseMax = basesMaxStr.split("\\|");
			limMaxUse = true;
		} else {
			baseMax = null;
			limMaxUse = false;
		}

		if (!limMinUse && !limMaxUse) {
			return sequences;
		}

		if ((limMinUse && (bases.length != baseMin.length)) || (limMaxUse && (bases.length != baseMax.length))) {
			throw new RuntimeException("Incorrect number of parameters");
		}

		if (sequences.first().getIsPaired()) {
			return sequences.filter(s -> this.filter(s, bases, baseMin, limMinUse, baseMax, limMaxUse)
					&& this.filterPair(s, bases, baseMin, limMinUse, baseMax, limMaxUse));
		}

		return sequences.filter(s -> this.filter(s, bases, baseMin, limMinUse, baseMax, limMaxUse));
	}

	/**
	 * Filter.
	 *
	 * @param sequence  the sequence
	 * @param bases     the bases
	 * @param baseMin   the base min
	 * @param limMinUse the lim min use
	 * @param baseMax   the base max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filter(Sequence sequence, String[] bases, String[] baseMin, Boolean limMinUse, String[] baseMax,
			Boolean limMaxUse) {
		return this.compare(sequence.getSequenceString(), bases, baseMin, limMinUse, baseMax, limMaxUse);
	}

	/**
	 * Filter pair.
	 *
	 * @param sequence  the sequence
	 * @param bases     the bases
	 * @param baseMin   the base min
	 * @param limMinUse the lim min use
	 * @param baseMax   the base max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filterPair(Sequence sequence, String[] bases, String[] baseMin, Boolean limMinUse, String[] baseMax,
			Boolean limMaxUse) {
		return this.compare(sequence.getSequenceStringPair(), bases, baseMin, limMinUse, baseMax, limMaxUse);
	}

	/**
	 * Compare.
	 *
	 * @param sequenceString the sequence string
	 * @param bases          the bases
	 * @param baseMin        the base min
	 * @param limMinUse      the lim min use
	 * @param baseMax        the base max
	 * @param limMaxUse      the lim max use
	 * @return the boolean
	 */
	private Boolean compare(String sequenceString, String[] bases, String[] baseMin, Boolean limMinUse,
			String[] baseMax, Boolean limMaxUse) {

		Double lim1;
		Double lim2;

		for (int i = 0; i < bases.length; i++) {
			Double perc = (double) StringUtils.countMatches(sequenceString, bases[i]) / sequenceString.length();
			if (limMinUse) {
				lim1 = new Double(baseMin[i]);
				if ((lim1 != -1) && (perc < lim1)) {
					return false;
				}
			}
			if (limMaxUse) {
				lim2 = new Double(baseMax[i]);
				if ((lim2 != -1) && (perc > lim2)) {
					return false;
				}
			}
		}

		return true;
	}
}