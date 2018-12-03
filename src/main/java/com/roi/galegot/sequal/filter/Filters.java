package com.roi.galegot.sequal.filter;

/**
 * The Enum Filters.
 */
public enum Filters {

	/** The length. */
	LENGTH(0, "com.roi.galegot.sequal.filter.single.Length"),

	/** The qualityscore. */
	QUALITYSCORE(1, "com.roi.galegot.sequal.filter.single.QualityScore"),

	/** The quality. */
	QUALITY(2, "com.roi.galegot.sequal.filter.single.Quality"),

	/** The gc. */
	GC(3, "com.roi.galegot.sequal.filter.single.GC"),

	/** The gcp. */
	GCP(4, "com.roi.galegot.sequal.filter.single.GCP"),

	/** The namb. */
	NAMB(5, "com.roi.galegot.sequal.filter.single.NAmb"),

	/** The nambp. */
	NAMBP(6, "com.roi.galegot.sequal.filter.single.NAmbP"),

	/** The noniupac. */
	NONIUPAC(7, "com.roi.galegot.sequal.filter.single.NonIupac"),

	/** The pattern. */
	PATTERN(8, "com.roi.galegot.sequal.filter.single.Pattern"),

	/** The nopattern. */
	NOPATTERN(9, "com.roi.galegot.sequal.filter.single.NoPattern"),

	/** The basen. */
	BASEN(10, "com.roi.galegot.sequal.filter.single.BaseN"),

	/** The basep. */
	BASEP(11, "com.roi.galegot.sequal.filter.single.BaseP"),

	/** The distinct. */
	DISTINCT(12, "com.roi.galegot.sequal.filter.group.Distinct"),

	/** The almostdistinct. */
	ALMOSTDISTINCT(13, "com.roi.galegot.sequal.filter.group.AlmostDistinct"),

	/** The reversedistinct. */
	REVERSEDISTINCT(14, "com.roi.galegot.sequal.filter.group.ReverseDistinct"),

	/** The complementdistinct. */
	COMPLEMENTDISTINCT(15, "com.roi.galegot.sequal.filter.group.ComplementDistinct"),

	/** The reversecomplementdistinct. */
	REVERSECOMPLEMENTDISTINCT(16, "com.roi.galegot.sequal.filter.group.ReverseComplementDistinct");

	/** The Constant MAX_FILTER_PRIORITY. */
	public static final int MAX_FILTER_PRIORITY = 16;

	/** The filter class name. */
	private String filterClassName;

	/** The priority. */
	private int priority;

	/**
	 * Instantiates a new filters.
	 *
	 * @param priority        the priority
	 * @param filterClassName the filter class name
	 */
	private Filters(int priority, String filterClassName) {
		this.priority = priority;
		this.filterClassName = filterClassName;
	}

	/**
	 * Gets the filter class.
	 *
	 * @return the filter class
	 */
	public String getFilterClass() {
		return this.filterClassName;
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
