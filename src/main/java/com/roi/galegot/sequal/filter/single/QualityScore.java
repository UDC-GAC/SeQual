package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class QualityScore.
 */
public class QualityScore implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1986175935593684878L;

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

		limMinStr = ExecutionParametersManager.getParameter("QualityScoreMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("QualityScoreMaxVal");

		limMin = (limMinUse = StringUtils.isNotBlank(limMinStr)) ? new Double(limMinStr) : null;
		limMax = (limMaxUse = StringUtils.isNotBlank(limMaxStr)) ? new Double(limMaxStr) : null;

		if ((!limMinUse && !limMaxUse) || !sequences.first().getHasQuality()) {
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
		if (seq.getHasQuality()) {
			if (limMinUse && limMaxUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if ((qual > limMax) || (qual < limMin)) {
						return false;
					}
				}
			}
			if (limMinUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if (qual < limMin) {
						return false;
					}
				}
			}
			if (limMaxUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if (qual > limMax) {
						return false;
					}
				}
			}
			return true;
		} else {
			return false;
		}
	}

	private Boolean filterPair(Sequence seq, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {

		// TODO

		if (seq.getHasQuality()) {
			if (limMinUse && limMaxUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if ((qual > limMax) || (qual < limMin)) {
						return false;
					}
				}
			}
			if (limMinUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if (qual < limMin) {
						return false;
					}
				}
			}
			if (limMaxUse) {
				for (char c : seq.getQualityString().toCharArray()) {
					int qual = c - 33;
					if (qual > limMax) {
						return false;
					}
				}
			}
			return true;
		} else {
			return false;
		}
	}

}