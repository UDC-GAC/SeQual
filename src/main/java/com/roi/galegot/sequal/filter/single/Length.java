package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class Length.
 */
public class Length implements SingleFilter {

	private static final long serialVersionUID = 8350107142175010716L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
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

		limMinStr = ExecutionParametersManager.getParameter("LengthMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("LengthMaxVal");

		limMin = (limMinUse = StringUtils.isNotBlank(limMinStr)) ? new Integer(limMinStr) : null;
		limMax = (limMaxUse = StringUtils.isNotBlank(limMaxStr)) ? new Integer(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.filter(s -> this.filter(s, limMin, limMinUse, limMax, limMaxUse)
					&& this.filterPair(s, limMin, limMinUse, limMax, limMaxUse));
		}

		return sequences.filter(s -> this.filter(s, limMin, limMinUse, limMax, limMaxUse));
	}

	/**
	 * Filter.
	 *
	 * @param sequence  the sequence
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filter(Sequence sequence, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		return this.compare(sequence.getLength(), limMin, limMinUse, limMax, limMaxUse);
	}

	/**
	 * Filter pair.
	 *
	 * @param sequence  the sequence
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filterPair(Sequence sequence, Integer limMin, Boolean limMinUse, Integer limMax,
			Boolean limMaxUse) {
		return this.compare(sequence.getLengthPair(), limMin, limMinUse, limMax, limMaxUse);
	}

	/**
	 * Compare.
	 *
	 * @param length    the length
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean compare(int length, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((length >= limMin) && (length <= limMax));
		}
		if (limMinUse) {
			return (length >= limMin);
		}
		if (limMaxUse) {
			return (length <= limMax);
		}

		return true;
	}
}