package com.roi.galegot.sequal.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.filter.Filter;
import com.roi.galegot.sequal.filter.FilterFactory;
import com.roi.galegot.sequal.filter.Filters;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class FilterService.
 */
public class FilterService {

	/**
	 * Instantiates a new filter service.
	 */
	private FilterService() {
	}

	/**
	 * Filter.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public static JavaRDD<Sequence> filter(JavaRDD<Sequence> sequences) {
		List<Filters> filters = getFilters();
		if (filters.isEmpty()) {
			System.out.println("\n\nNo filters specified. No operations will be performed.");
		} else {
			sequences = applyFilters(sequences, filters);
		}
		return sequences;
	}

	/**
	 * Retrieves the specified filters into ExecutionParameter.properties file and
	 * converts them into Filters
	 *
	 * @return List<Filters> containing all the specified filters
	 * @see filters.Filters
	 */
	private static List<Filters> getFilters() {
		String filters;
		String[] splitFilters;
		Map<Integer, Filters> filtersMap;

		filtersMap = new TreeMap<Integer, Filters>();

		filters = ExecutionParametersManager.getParameter("SingleFilters");
		if (StringUtils.isNotBlank(filters)) {
			splitFilters = filters.split("\\|");
			for (String filter : splitFilters) {
				Filters filterEntity = Filters.valueOf(filter.trim());
				filtersMap.put(filterEntity.getPriority(), filterEntity);
			}
		}

		filters = ExecutionParametersManager.getParameter("GroupFilters");
		if (StringUtils.isNotBlank(filters)) {
			splitFilters = filters.split("\\|");
			for (String filter : splitFilters) {
				Filters filterEntity = Filters.valueOf(filter.trim());
				filtersMap.put(filterEntity.getPriority(), filterEntity);
			}
		}

		return new ArrayList<Filters>(filtersMap.values());
	}

	/**
	 * Apply filters.
	 *
	 * @param sequences the sequences
	 * @param filters   the filters
	 * @return the java RDD
	 */
	private static JavaRDD<Sequence> applyFilters(JavaRDD<Sequence> sequences, List<Filters> filters) {
		for (int i = 0; i < filters.size(); i++) {
			if (sequences.isEmpty()) {
				return sequences;
			}
			Filter filter = FilterFactory.getFilter(filters.get(i));
			sequences = filter.validate(sequences);
		}
		return sequences;
	}

}