package com.roi.galegot.sequal.sequalmodel.formatter;

/**
 * The Enum Formatters.
 */
public enum Formatters {

	/** The dnatorna. */
	DNATORNA("com.roi.galegot.sequal.sequalmodel.formatter.DNAToRNA"),

	/** The rnatodna. */
	RNATODNA("com.roi.galegot.sequal.sequalmodel.formatter.RNAToDNA"),

	/** The fastqtofasta. */
	FASTQTOFASTA("com.roi.galegot.sequal.sequalmodel.formatter.FASTQToFASTA");

	/** The formatter class name. */
	private String formatterClassName;

	/**
	 * Instantiates a new formatters.
	 *
	 * @param formatterClassName the formatter class name
	 */
	private Formatters(String formatterClassName) {
		this.formatterClassName = formatterClassName;
	}

	/**
	 * Gets the formatter class.
	 *
	 * @return the formatter class
	 */
	public String getFormatterClass() {
		return this.formatterClassName;
	}
}