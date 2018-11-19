package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

/**
 * The Class NonIupac.
 */
public class NonIupac implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -7681154534922631509L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		String[] bases = { "A", "C", "G", "T", "N" };

		if (sequences.isEmpty()) {
			return sequences;
		}

		return sequences.filter(s -> this.filter(s, bases));
	}

	/**
	 * Filter.
	 *
	 * @param seq   the seq
	 * @param bases the bases
	 * @return the boolean
	 */
	private Boolean filter(Sequence seq, String[] bases) {
		int counter;
		String sequence;

		counter = 0;
		sequence = seq.getSequenceString();

		for (String base : bases) {
			counter = counter + StringUtils.countMatches(sequence, base);
		}

		return (counter == sequence.length());
	}
}