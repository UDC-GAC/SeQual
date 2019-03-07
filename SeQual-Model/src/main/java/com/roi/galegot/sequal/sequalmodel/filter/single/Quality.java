package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class Quality.
 */
public class Quality implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6867361108805219701L;

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

		limMinStr = ExecutionParametersManager.getParameter(FilterParametersNaming.QUALITY_MIN_VAL);
		limMaxStr = ExecutionParametersManager.getParameter(FilterParametersNaming.QUALITY_MAX_VAL);

		limMin = (limMinUse = StringUtils.isNotBlank(limMinStr)) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = StringUtils.isNotBlank(limMaxStr)) ? new Double(limMaxStr) : null;

		if ((!limMinUse && !limMaxUse) || !sequences.first().getHasQuality()) {
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
	private Boolean filter(Sequence sequence, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (sequence.getHasQuality()) {
			return this.compare(sequence.getQuality(), limMin, limMinUse, limMax, limMaxUse);
		}

		return false;
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
	private Boolean filterPair(Sequence sequence, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (sequence.getHasQuality()) {
			return this.compare(sequence.getQualityPair(), limMin, limMinUse, limMax, limMaxUse);
		}

		return false;
	}

	/**
	 * Compare.
	 *
	 * @param quality   the quality
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean compare(double quality, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			return ((quality >= limMin) && (quality <= limMax));
		}
		if (limMinUse) {
			return (quality >= limMin);
		}
		if (limMaxUse) {
			return (quality <= limMax);
		}
		return true;
	}
}