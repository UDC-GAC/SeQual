package com.roi.galegot.sequal.exceptions;

/**
 * The Class NonExistentFormatterException.
 */
@SuppressWarnings("serial")
public class NonExistentTrimmerException extends RuntimeException {

	/**
	 * Instantiates a new non existent formatter exception.
	 *
	 * @param formatter the formatter
	 */
	public NonExistentTrimmerException(String trimmer) {
		super("Specified trimmer " + trimmer + " does not exist.");
	}
}