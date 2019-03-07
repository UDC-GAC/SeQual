package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class QualityScore.
 */
public class QualityScore implements SingleFilter {

	private static final long serialVersionUID = 1986175935593684878L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
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

		limMinStr = ExecutionParametersManager.getParameter(FilterParametersNaming.QUALITY_SCORE_MIN_VAL);
		limMaxStr = ExecutionParametersManager.getParameter(FilterParametersNaming.QUALITY_SCORE_MAX_VAL);

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
			return this.compare(sequence.getQualityString(), limMin, limMinUse, limMax, limMaxUse);
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
			return this.compare(sequence.getQualityStringPair(), limMin, limMinUse, limMax, limMaxUse);
		}

		return false;
	}

	/**
	 * Compare.
	 *
	 * @param qualityString the quality string
	 * @param limMin        the lim min
	 * @param limMinUse     the lim min use
	 * @param limMax        the lim max
	 * @param limMaxUse     the lim max use
	 * @return the boolean
	 */
	private Boolean compare(String qualityString, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			for (char c : qualityString.toCharArray()) {
				int qual = c - 33;
				if ((qual > limMax) || (qual < limMin)) {
					return false;
				}
			}
		}
		if (limMinUse) {
			for (char c : qualityString.toCharArray()) {
				int qual = c - 33;
				if (qual < limMin) {
					return false;
				}
			}
		}
		if (limMaxUse) {
			for (char c : qualityString.toCharArray()) {
				int qual = c - 33;
				if (qual > limMax) {
					return false;
				}
			}
		}

		return true;
	}

}