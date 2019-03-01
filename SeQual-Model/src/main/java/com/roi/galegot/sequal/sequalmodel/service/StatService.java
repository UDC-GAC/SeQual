package com.roi.galegot.sequal.sequalmodel.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.exceptions.NonExistentStatException;
import com.roi.galegot.sequal.sequalmodel.stat.Stat;
import com.roi.galegot.sequal.sequalmodel.stat.StatFactory;
import com.roi.galegot.sequal.sequalmodel.stat.Stats;
import com.roi.galegot.sequal.sequalmodel.stat.StatsNaming;
import com.roi.galegot.sequal.sequalmodel.stat.StatsPhrasing;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class StatsService.
 */
public class StatService {

	private static final Logger LOGGER = Logger.getLogger(StatService.class.getName());

	private Map<String, Double> results = new HashMap<String, Double>();
	private Map<String, Double> resultsPair = new HashMap<String, Double>();

	private Boolean isPaired = false;

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

		if (!seqs.isEmpty() && seqs.first().getIsPaired()) {
			this.isPaired = true;
		}

		if (!stats.isEmpty()) {
			for (int i = 0; i < stats.size(); i++) {
				Stat stat = StatFactory.getStat(stats.get(i));

				LOGGER.info("\nApplying measurement " + stats.get(i));

				this.addToResult(stats.get(i).toString(), stat.measure(seqs), isFirst, false);

				if (this.isPaired) {
					this.addToResult(stats.get(i).toString(), stat.measurePair(seqs), isFirst, true);
				}
			}
		} else {
			if (isFirst) {
				LOGGER.warn("\nNo statistics specified. No measurements will be performed.\n");
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
	 * Gets the results pair.
	 *
	 * @return the results pair
	 */
	public Map<String, Double> getResultsPair() {
		return this.resultsPair;
	}

	/**
	 * Gets the results as string.
	 *
	 * @return the results as string
	 */
	public String getResultsAsString() {
		String resultString = "";

		if (!this.isPaired) {
			for (String statPhrase : StatsPhrasing.ORDERED_STATS) {
				if (this.results.containsKey(statPhrase)) {
					resultString = resultString.concat(statPhrase + " " + this.results.get(statPhrase) + "\n");
				}
			}
		} else {
			resultString = resultString.concat("Results for first file:\n");
			for (String statPhrase : StatsPhrasing.ORDERED_STATS) {
				if (this.results.containsKey(statPhrase)) {
					resultString = resultString.concat(statPhrase + " " + this.results.get(statPhrase) + "\n");
				}
			}

			resultString = resultString.concat("\nResults for second file:\n");
			for (String statPhrase : StatsPhrasing.ORDERED_STATS) {
				if (this.resultsPair.containsKey(statPhrase)) {
					resultString = resultString.concat(statPhrase + " " + this.results.get(statPhrase) + "\n");
				}
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
	private void addToResult(String statName, Double result, Boolean isFirst, Boolean pair) {

		switch (statName) {
		case StatsNaming.COUNT:
			if (isFirst) {
				this.results.put(StatsPhrasing.COUNT_BEFORE, result);
				if (pair) {
					this.resultsPair.put(StatsPhrasing.COUNT_BEFORE, result);
				}
			} else {
				this.results.put(StatsPhrasing.COUNT_AFTER, result);
				if (pair) {
					this.resultsPair.put(StatsPhrasing.COUNT_AFTER, result);
				}
			}
			break;

		case StatsNaming.MEAN_LENGTH:
			if (isFirst) {
				this.results.put(StatsPhrasing.MEAN_LENGTH_BEFORE, result);
				if (pair) {
					this.resultsPair.put(StatsPhrasing.MEAN_LENGTH_BEFORE, result);
				}
			} else {
				this.results.put(StatsPhrasing.MEAN_LENGTH_AFTER, result);
				if (pair) {
					this.resultsPair.put(StatsPhrasing.MEAN_LENGTH_AFTER, result);
				}
			}
			break;

		case StatsNaming.MEAN_QUALITY:
			if (isFirst) {
				this.results.put(StatsPhrasing.MEAN_QUALITY_BEFORE, result);
				if (pair) {
					this.resultsPair.put(StatsPhrasing.MEAN_QUALITY_BEFORE, result);
				}
			} else {
				this.results.put(StatsPhrasing.MEAN_QUALITY_AFTER, result);
				if (pair) {
					this.resultsPair.put(StatsPhrasing.MEAN_QUALITY_AFTER, result);
				}
			}
			break;

		default:
			throw new NonExistentStatException(statName);
		}
	}

}