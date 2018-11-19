package com.roi.galegot.sequal.filter;

public class FilterFactory {
	private FilterFactory() {
	}

	/**
	 * Returns a filter based on the enum Filters
	 *
	 * @param filter Filter selected to be returned
	 * @return SingleFilter specified
	 */
	public synchronized static Filter getFilter(Filters filter) {
		try {
			return (Filter) Class.forName(filter.getFilterClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Filter not found");
		}
	}
}
