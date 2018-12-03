package com.roi.galegot.sequal.exceptions;

/**
 * The Class NonExistentFormatterException.
 */
@SuppressWarnings("serial")
public class NonExistentFormatterException extends RuntimeException {

	/**
	 * Instantiates a new non existent formatter exception.
	 *
	 * @param formatter the formatter
	 */
	public NonExistentFormatterException(String formatter) {
		super("Specified formatter " + formatter + " does not exist.");
	}
}