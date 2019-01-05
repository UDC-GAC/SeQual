package com.roi.galegot.sequal.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.exceptions.NonExistentStatException;
import com.roi.galegot.sequal.stat.Stat;
import com.roi.galegot.sequal.stat.StatFactory;
import com.roi.galegot.sequal.stat.Stats;
import com.roi.galegot.sequal.stat.StatsNaming;
import com.roi.galegot.sequal.stat.StatsPhrasing;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class StatsService.
 */
public class StatService {

	/** The results. */
	private static Map<String, Double> results = new HashMap<String, Double>();

	/**
	 * Instantiates a new stats service.
	 */
	public StatService() {
	}

	/**
	 * Measure.
	 *
	 * @param seqs    the seqs
	 * @param isFirst the is first
	 */
	public static void measure(JavaRDD<Sequence> seqs, Boolean isFirst) {
		List<Stats> stats = getStats();

		if (isFirst) {
			results = new HashMap<String, Double>();
		}

		if (!stats.isEmpty()) {
			for (int i = 0; i < stats.size(); i++) {
				Stat stat = StatFactory.getStat(stats.get(i));
				addToResult(stats.get(i).toString(), stat.measure(seqs),
						isFirst);
			}
		} else {
			if (isFirst) {
				System.out.println(
						"\nNo statistics specified. No measurements will be performed.\n");
			}
		}
	}

	/**
	 * Gets the stats.
	 *
	 * @return the stats
	 */
	private static List<Stats> getStats() {
		String stats = ExecutionParametersManager.getParameter("Statistics");
		ArrayList<Stats> enumStats = new ArrayList<>();
		if (StringUtils.isNotBlank(stats)) {
			String[] splitStats = stats.split("\\|");
			for (String stat : splitStats) {
				enumStats.add(Stats.valueOf(stat.trim()));
			}
		}
		return enumStats;
	}

	/**
	 * Gets the results.
	 *
	 * @return the results
	 */
	public static Map<String, Double> getResults() {
		return results;
	}

	/**
	 * Gets the results as string.
	 *
	 * @return the results as string
	 */
	public static String getResultsAsString() {
		String resultString = "";

		for (String statPhrase : StatsPhrasing.ORDERED_STATS) {
			if (results.containsKey(statPhrase)) {
				resultString = resultString.concat(
						statPhrase + " " + results.get(statPhrase) + "\n");
			}
		}

		return resultString;
	}

	/**
	 * Adds the to result.
	 *
	 * @param statName the stat name
	 * @param result   the result
	 * @param isFirst  the is first
	 */
	private static void addToResult(String statName, Double result,
			Boolean isFirst) {

		switch (statName) {
		case StatsNaming.COUNT:
			if (isFirst) {
				results.put(StatsPhrasing.COUNT_BEFORE, result);
			} else {
				results.put(StatsPhrasing.COUNT_AFTER, result);
			}
			break;

		case StatsNaming.MEAN_LENGTH:
			if (isFirst) {
				results.put(StatsPhrasing.MEAN_LENGTH_BEFORE, result);
			} else {
				results.put(StatsPhrasing.MEAN_LENGTH_AFTER, result);
			}
			break;

		case StatsNaming.MEAN_QUALITY:
			if (isFirst) {
				results.put(StatsPhrasing.MEAN_QUALITY_BEFORE, result);
			} else {
				results.put(StatsPhrasing.MEAN_QUALITY_AFTER, result);
			}
			break;

		default:
			throw new NonExistentStatException(statName);
		}
	}

}