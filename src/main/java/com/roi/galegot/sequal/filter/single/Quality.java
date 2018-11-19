package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

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

		limMinStr = ExecutionParametersManager.getParameter("QualityMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("QualityMaxVal");

		limMin = (limMinUse = StringUtils.isNotBlank(limMinStr)) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = StringUtils.isNotBlank(limMaxStr)) ? new Double(limMaxStr) : null;

		if ((!limMinUse && !limMaxUse) || !sequences.first().isHasQual()) {
			return sequences;
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
		if (seq.isHasQual()) {
			if (limMinUse && limMaxUse) {
				return ((seq.getQuality() >= limMin) && (seq.getQuality() <= limMax));
			}
			if (limMinUse) {
				return (seq.getQuality() >= limMin);
			}
			if (limMaxUse) {
				return (seq.getQuality() <= limMax);
			}
			return true;
		} else {
			return false;
		}
	}
}