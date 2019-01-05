package com.roi.galegot.sequal.stat;

/**
 * The Enum Stats.
 */
public enum Stats {

	/** The count. */
	COUNT("com.roi.galegot.sequal.stats.Count", StatsNaming.COUNT),

	/** The meanlength. */
	MEANLENGTH("com.roi.galegot.sequal.stats.MeanLength",
			StatsNaming.MEAN_LENGTH),

	/** The meanquality. */
	MEANQUALITY("com.roi.galegot.sequal.stats.MeanQuality",
			StatsNaming.MEAN_QUALITY);

	/** The stat class name. */
	private String statClassName;

	/** The stat name. */
	private String statName;

	/**
	 * Instantiates a new stats.
	 *
	 * @param statClassName the stat class name
	 * @param statName      the stat name
	 */
	private Stats(String statClassName, String statName) {
		this.statClassName = statClassName;
	}

	/**
	 * Gets the stat class.
	 *
	 * @return the stat class
	 */
	public String getStatClass() {
		return this.statClassName;
	}

	/**
	 * Gets the stat name.
	 *
	 * @return the stat name
	 */
	public String getStatName() {
		return this.statName;
	}
}