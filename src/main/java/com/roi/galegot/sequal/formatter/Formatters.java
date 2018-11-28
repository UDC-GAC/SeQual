package com.roi.galegot.sequal.formatter;

public enum Formatters {
	DNATORNA("formatters.DNAToRNA"), RNATODNA("formatters.RNAToDNA"), FASTQTOFASTA("formatters.FASTQToFASTA");

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