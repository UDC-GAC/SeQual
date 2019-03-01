package com.roi.galegot.sequal.sequalmodel.formatter;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Class DNAToRNA.
 */
public class DNAToRNA implements Formatter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1507230060510674227L;

	@Override
	public JavaRDD<Sequence> format(JavaRDD<Sequence> sequences) {

		if (sequences.first().getIsPaired()) {
			return sequences.map(sequence -> this.doFormatPair(sequence));
		}

		return sequences.map(sequence -> this.doFormat(sequence));
	}

	/**
	 * Do format.
	 *
	 * @param sequence the sequence
	 * @return the sequence
	 */
	private Sequence doFormat(Sequence sequence) {
		sequence.setSequenceString(sequence.getSequenceString().replace("T", "U"));
		return sequence;
	}

	/**
	 * Do format pair.
	 *
	 * @param sequence the sequence
	 * @return the sequence
	 */
	private Sequence doFormatPair(Sequence sequence) {

		this.doFormat(sequence);

		sequence.setSequenceStringPair(sequence.getSequenceStringPair().replace("T", "U"));
		return sequence;
	}

}