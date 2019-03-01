package com.roi.galegot.sequal.sequalmodel.exceptions;

/**
 * The Class NonExistentFormatterException.
 */
@SuppressWarnings("serial")
public class NonExistentStatException extends RuntimeException {

	/**
	 * Instantiates a new non existent stat exception.
	 *
	 * @param stat the stat
	 */
	public NonExistentStatException(String stat) {
		super("Specified stat " + stat + " does not exist.");
	}
}