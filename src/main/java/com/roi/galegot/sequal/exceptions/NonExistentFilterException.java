package com.roi.galegot.sequal.exceptions;

/**
 * The Class NonExistentFilterException.
 */
@SuppressWarnings("serial")
public class NonExistentFilterException extends RuntimeException {

	/**
	 * Instantiates a new non existent filter exception.
	 *
	 * @param filter the filter
	 */
	public NonExistentFilterException(String filter) {
		super("Specified filter " + filter + " does not exist.");
	}
}