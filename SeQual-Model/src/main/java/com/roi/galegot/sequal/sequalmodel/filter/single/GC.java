package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class GC.
 */
public class GC implements SingleFilter {

	private static final long serialVersionUID = 8628696067644155529L;

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

		limMinStr = ExecutionParametersManager.getParameter("GCMinVal");
		limMaxStr = ExecutionParametersManager.getParameter("GCMaxVal");

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
		return this.compare(sequence.getGuaCyt(), limMin, limMinUse, limMax, limMaxUse);
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
		return this.compare(sequence.getGuaCytPair(), limMin, limMinUse, limMax, limMaxUse);
	}

	/**
	 * Compare.
	 *
	 * @param guaCyt    the gua cyt
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean compare(int guaCyt, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {

		if (limMinUse && limMaxUse) {
			return ((guaCyt >= limMin) && (guaCyt <= limMax));
		}
		if (limMinUse) {
			return (guaCyt >= limMin);
		}
		if (limMaxUse) {
			return (guaCyt <= limMax);
		}

		return true;
	}
}