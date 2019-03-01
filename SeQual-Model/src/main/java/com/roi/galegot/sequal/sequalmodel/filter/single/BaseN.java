package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class BaseN.
 */
public class BaseN implements SingleFilter {

	private static final long serialVersionUID = -114406829239103678L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
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

		String basesStr = ExecutionParametersManager.getParameter("Base");
		if (StringUtils.isNotBlank(basesStr)) {
			bases = basesStr.split("\\|");
		} else {
			return sequences;
		}

		basesMinStr = ExecutionParametersManager.getParameter("BaseMinVal");
		if (StringUtils.isNotBlank(basesMinStr)) {
			baseMin = basesMinStr.split("\\|");
			limMinUse = true;
		} else {
			baseMin = null;
			limMinUse = false;
		}

		basesMaxStr = ExecutionParametersManager.getParameter("BaseMaxVal");
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

		Integer lim1;
		Integer lim2;

		for (int i = 0; i < bases.length; i++) {
			int reps = StringUtils.countMatches(sequenceString, bases[i]);
			if (limMinUse) {
				lim1 = new Integer(baseMin[i]);
				if ((lim1 != -1) && (reps < lim1)) {
					return false;
				}
			}
			if (limMaxUse) {
				lim2 = new Integer(baseMax[i]);
				if ((lim2 != -1) && (reps > lim2)) {
					return false;
				}
			}
		}

		return true;
	}
}