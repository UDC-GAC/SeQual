package com.roi.galegot.sequal.sequalmodel.formatter;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Class FASTQToFASTA.
 */
public class FASTQToFASTA implements Formatter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5337524582924746654L;

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

		sequence.setName(">" + sequence.getName().substring(1));
		sequence.setQualityString("");

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

		sequence.setNamePair(">" + sequence.getNamePair().substring(1));
		sequence.setQualityStringPair("");

		return sequence;
	}
}