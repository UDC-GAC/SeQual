package com.roi.galegot.sequal.sequalmodel.formatter;

import com.roi.galegot.sequal.sequalmodel.exceptions.NonExistentFormatterException;

/**
 * A factory for creating Formatter objects.
 */
public class FormatterFactory {

	/**
	 * Instantiates a new formatter factory.
	 */
	private FormatterFactory() {
	}

	/**
	 * Returns a Formatter based on the enum Formatters.
	 *
	 * @param formatter Formatter selected to be returned
	 * @return Formatter specified
	 */
	public static synchronized Formatter getFormatter(Formatters formatter) {
		try {
			return (Formatter) Class.forName(formatter.getFormatterClass())
					.newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new NonExistentFormatterException(
					formatter.getFormatterClass());
		}
	}
}