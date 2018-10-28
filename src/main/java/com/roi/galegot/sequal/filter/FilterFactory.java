package com.roi.galegot.sequal.filter;

import com.roi.galegot.sequal.filter.group.GroupFilter;
import com.roi.galegot.sequal.filter.single.SingleFilter;

public class FilterFactory {
	private FilterFactory() {
	}

	/**
	 * Returns a single filter based on the enum Filters
	 *
	 * @param filter Filter selected to be returned
	 * @return SingleFilter specified
	 */
	public synchronized static SingleFilter getSingleFilter(Filters filter) {
		try {
			return (SingleFilter) Class.forName(filter.getFilterClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Filter not found");
		}
	}

	/**
	 * Returns a group filter based on the enum Filters
	 *
	 * @param filter Filter selected to be returned
	 * @return GroupFilter specified
	 */
	public synchronized static GroupFilter getGroupFilter(Filters filter) {
		try {
			return (GroupFilter) Class.forName(filter.getFilterClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Filter not found");
		}
	}
}
