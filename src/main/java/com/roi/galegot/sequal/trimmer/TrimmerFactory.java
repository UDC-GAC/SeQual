package com.roi.galegot.sequal.trimmer;

import com.roi.galegot.sequal.exceptions.NonExistentTrimmerException;

/**
 * A factory for creating Trimmer objects.
 */
public class TrimmerFactory {

	/**
	 * Instantiates a new trimmer factory.
	 */
	private TrimmerFactory() {
	}

	/**
	 * Returns a Trimmer based on the enum Trimmers.
	 *
	 * @param trimmer Trimmer selected to be returned
	 * @return Trimmer specified
	 */
	public static synchronized Trimmer getTrimmer(Trimmers trimmer) {
		try {
			return (Trimmer) Class.forName(trimmer.getTrimmerClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new NonExistentTrimmerException(trimmer.getTrimmerClass());
		}
	}
}