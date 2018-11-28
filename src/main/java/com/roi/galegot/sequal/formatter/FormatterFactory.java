package com.roi.galegot.sequal.formatter;

public class FormatterFactory {

	private FormatterFactory() {
	}

	/**
	 * Returns a Formatter based on the enum Formatters.
	 *
	 * @param formatter Formatter selected to be returned
	 * @return Formatter specified
	 */
	public synchronized static Formatter getFormatter(Formatters formatter) {
		try {
			return (Formatter) Class.forName(formatter.getFormatterClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Formatter not found");
		}
	}
}