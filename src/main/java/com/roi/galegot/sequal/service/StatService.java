package com.roi.galegot.sequal.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.console.ConsoleInterface;
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

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger
			.getLogger(ConsoleInterface.class.getName());

	/** The results. */
	private Map<String, Double> results = new HashMap<String, Double>();

	/**
	 * Measure.
	 *
	 * @param seqs    the seqs
	 * @param isFirst the is first
	 */
	public void measure(JavaRDD<Sequence> seqs, Boolean isFirst) {
		List<Stats> stats = this.getStats();

		if (isFirst) {
			this.results = new HashMap<String, Double>();
		}

		if (!stats.isEmpty()) {
			for (int i = 0; i < stats.size(); i++) {
				Stat stat = StatFactory.getStat(stats.get(i));

				LOGGER.info("Applying measurement " + stats.get(i));

				this.addToResult(stats.get(i).toString(), stat.measure(seqs),
						isFirst);
			}
		} else {
			if (isFirst) {
				LOGGER.warn(
						"\nNo statistics specified. No measurements will be performed.\n");
			}
		}
	}

	/**
	 * Gets the stats.
	 *
	 * @return the stats
	 */
	private List<Stats> getStats() {
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
	public Map<String, Double> getResults() {
		return this.results;
	}

	/**
	 * Gets the results as string.
	 *
	 * @return the results as string
	 */
	public String getResultsAsString() {
		String resultString = "";

		for (String statPhrase : StatsPhrasing.ORDERED_STATS) {
			if (this.results.containsKey(statPhrase)) {
				resultString = resultString.concat(
						statPhrase + " " + this.results.get(statPhrase) + "\n");
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
	private void addToResult(String statName, Double result, Boolean isFirst) {

		switch (statName) {
		case StatsNaming.COUNT:
			if (isFirst) {
				this.results.put(StatsPhrasing.COUNT_BEFORE, result);
			} else {
				this.results.put(StatsPhrasing.COUNT_AFTER, result);
			}
			break;

		case StatsNaming.MEAN_LENGTH:
			if (isFirst) {
				this.results.put(StatsPhrasing.MEAN_LENGTH_BEFORE, result);
			} else {
				this.results.put(StatsPhrasing.MEAN_LENGTH_AFTER, result);
			}
			break;

		case StatsNaming.MEAN_QUALITY:
			if (isFirst) {
				this.results.put(StatsPhrasing.MEAN_QUALITY_BEFORE, result);
			} else {
				this.results.put(StatsPhrasing.MEAN_QUALITY_AFTER, result);
			}
			break;

		default:
			throw new NonExistentStatException(statName);
		}
	}

}