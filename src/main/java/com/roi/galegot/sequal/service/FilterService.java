package com.roi.galegot.sequal.service;

/**
 * @author Roi Galego
 *
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.filter.FilterFactory;
import com.roi.galegot.sequal.filter.Filters;
import com.roi.galegot.sequal.filter.group.GroupFilter;
import com.roi.galegot.sequal.filter.single.SingleFilter;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class FilterService {

	private FilterService() {
	}

	public static JavaRDD<Sequence> filter(JavaRDD<Sequence> seqs) {
		List<Filters> singleFilters = getFilters("SingleFilters");
		List<Filters> groupFilters = getFilters("GroupFilters");
		if (!(singleFilters.isEmpty() && groupFilters.isEmpty())) {
			seqs = singleFilter(seqs, singleFilters);
			seqs = groupFilter(seqs, groupFilters);
		} else {
			System.out.println("\n\nNo filters specified. No operations will be performed.");
		}
		return seqs;
	}

	/**
	 * Reads the input file, transforms its content into a JavaRDD of Sequences,
	 * applies every single filter specified into ExecutionParameter.properties file
	 * and writes the results in the output directory
	 *
	 * @param inFile  path to the input file
	 * @param outFile path to where to write the results
	 * @throws IOException
	 */
	private static JavaRDD<Sequence> singleFilter(JavaRDD<Sequence> seqs, List<Filters> filters) {
		for (int i = 0; i < filters.size(); i++) {
			SingleFilter filter = FilterFactory.getSingleFilter(filters.get(i));
			seqs = filter.validate(seqs);
		}
		return seqs;
	}

	/**
	 * Reads the input file, transforms its content into a JavaRDD of Sequences,
	 * applies every group filter specified into ExecutionParameter.properties file
	 * and writes the results in the output directory
	 *
	 * @param inFile  path to the input file
	 * @param outFile path to where to write the results
	 * @throws IOException
	 */
	private static JavaRDD<Sequence> groupFilter(JavaRDD<Sequence> seqs, List<Filters> filters) {
		for (int i = 0; i < filters.size(); i++) {
			GroupFilter filter = FilterFactory.getGroupFilter(filters.get(i));
			seqs = filter.validate(seqs);
		}
		return seqs;
	}

	/**
	 * Retrieves the specified filters into ExecutionParameter.properties file and
	 * converts them into Filters
	 *
	 * @param type specifies whether to pick single or group filters
	 * @return List<Filters> containing all the specified filters
	 * @see filters.Filters
	 */
	private static List<Filters> getFilters(String type) {
		String filters = ExecutionParametersManager.getParameter(type);
		String[] splitFilters = filters.split("\\|");
		ArrayList<Filters> enumFilters = new ArrayList<>();
		if (!filters.isEmpty()) {
			for (String filter : splitFilters) {
				enumFilters.add(Filters.valueOf(filter.trim()));
			}
		}
		return enumFilters;
	}
}