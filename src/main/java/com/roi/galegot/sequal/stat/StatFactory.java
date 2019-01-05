package com.roi.galegot.sequal.stat;

import com.roi.galegot.sequal.exceptions.NonExistentStatException;

/**
 * A factory for creating Stat objects.
 */
public class StatFactory {

	/**
	 * Instantiates a new stat factory.
	 */
	private StatFactory() {
	}

	/**
	 * Returns a Stat based on the enum Stats.
	 *
	 * @param stat Stat selected to be returned
	 * @return Stat specified
	 */
	public synchronized static Stat getStat(Stats stat) {
		try {
			return (Stat) Class.forName(stat.getStatClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new NonExistentStatException(stat.getStatClass());
		}
	}
}