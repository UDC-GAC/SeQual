package com.roi.galegot.sequal.stat;

/**
 * The Enum Stats.
 */
public enum Stats {

	/** The count. */
	COUNT("com.roi.galegot.sequal.stat.Count"),

	/** The meanlength. */
	MEANLENGTH("com.roi.galegot.sequal.stat.MeanLength"),

	/** The meanquality. */
	MEANQUALITY("com.roi.galegot.sequal.stat.MeanQuality");

	/** The stat class name. */
	private String statClassName;

	/**
	 * Instantiates a new stats.
	 *
	 * @param statClassName the stat class name
	 * @param statName      the stat name
	 */
	private Stats(String statClassName) {
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
}