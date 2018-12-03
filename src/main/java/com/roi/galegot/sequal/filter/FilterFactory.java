package com.roi.galegot.sequal.filter;

import com.roi.galegot.sequal.exceptions.NonExistentFilterException;

/**
 * A factory for creating Filter objects.
 */
public class FilterFactory {

	/**
	 * Instantiates a new filter factory.
	 */
	private FilterFactory() {
	}

	/**
	 * Returns a filter based on the enum Filters.
	 *
	 * @param filter Filter selected to be returned
	 * @return SingleFilter specified
	 */
	public synchronized static Filter getFilter(Filters filter) {
		try {
			return (Filter) Class.forName(filter.getFilterClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new NonExistentFilterException(filter.getFilterClass());
		}
	}
}
