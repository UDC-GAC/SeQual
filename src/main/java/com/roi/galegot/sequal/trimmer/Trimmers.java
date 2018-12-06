package com.roi.galegot.sequal.trimmer;

/**
 * The Enum Trimmers.
 */
public enum Trimmers {

	/** The trimleft. */
	TRIMLEFT(0, "trimmers.TrimLeft"),

	/** The trimright. */
	TRIMRIGHT(1, "trimmers.TrimRight"),

	/** The trimleftp. */
	TRIMLEFTP(2, "trimmers.TrimLeftP"),

	/** The trimrightp. */
	TRIMRIGHTP(3, "trimmers.TrimRightP"),

	/** The trimqualleft. */
	TRIMQUALLEFT(4, "trimmers.TrimQualLeft"),

	/** The trimqualright. */
	TRIMQUALRIGHT(5, "trimmers.TrimQualRight"),

	/** The trimleft. */
	TRIMNLEFT(6, "trimmers.TrimNLeft"),

	/** The trimright. */
	TRIMNRIGHT(7, "trimmers.TrimNRight"),

	/** The trimlefttolength. */
	TRIMLEFTTOLENGTH(8, "trimmers.TrimLeftToLength"),

	/** The trimrighttolength. */
	TRIMRIGHTTOLENGTH(9, "trimmers.TrimRightToLength");

	/** The Constant MAX_FILTER_PRIORITY. */
	public static final int MAX_TRIMMER_PRIORITY = 9;

	/** The trimmer class name. */
	private String trimmerClassName;

	/** The priority. */
	private int priority;

	/**
	 * Instantiates a new trimmers.
	 *
	 * @param priority         the priority
	 * @param trimmerClassName the trimmer class name
	 */
	private Trimmers(int priority, String trimmerClassName) {
		this.priority = priority;
		this.trimmerClassName = trimmerClassName;
	}

	/**
	 * Gets the trimmer class.
	 *
	 * @return the trimmer class
	 */
	public String getTrimmerClass() {
		return this.trimmerClassName;
	}

	/**
	 * Gets the priority.
	 *
	 * @return the priority
	 */
	public int getPriority() {
		return this.priority;
	}
}