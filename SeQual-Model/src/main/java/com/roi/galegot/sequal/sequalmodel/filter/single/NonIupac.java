package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Class NonIupac.
 */
public class NonIupac implements SingleFilter {

	private static final long serialVersionUID = -7681154534922631509L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		String[] bases = { "A", "C", "G", "T", "N" };

		if (sequences.isEmpty()) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.filter(s -> this.filter(s, bases) && this.filterPair(s, bases));
		}

		return sequences.filter(s -> this.filter(s, bases));
	}

	/**
	 * Filter.
	 *
	 * @param sequence the sequence
	 * @param bases    the bases
	 * @return the boolean
	 */
	private Boolean filter(Sequence sequence, String[] bases) {
		return this.compare(sequence.getSequenceString(), bases);
	}

	/**
	 * Filter pair.
	 *
	 * @param sequence the sequence
	 * @param bases    the bases
	 * @return the boolean
	 */
	private Boolean filterPair(Sequence sequence, String[] bases) {
		return this.compare(sequence.getSequenceStringPair(), bases);
	}

	/**
	 * Compare.
	 *
	 * @param sequenceString the sequence string
	 * @param bases          the bases
	 * @return the boolean
	 */
	private Boolean compare(String sequenceString, String[] bases) {

		int counter;

		counter = 0;

		for (String base : bases) {
			counter = counter + StringUtils.countMatches(sequenceString, base);
		}

		return (counter == sequenceString.length());
	}
}