package com.roi.galegot.sequal.filter;

public enum Filters {
	LENGTH(0, "com.roi.galegot.sequal.filter.single.Length"),
	QUALITYSCORE(1, "com.roi.galegot.sequal.filter.single.QualityScore"),
	QUALITY(2, "com.roi.galegot.sequal.filter.single.Quality"), GC(3, "com.roi.galegot.sequal.filter.single.GC"),
	GCP(4, "com.roi.galegot.sequal.filter.single.GCP"), NAMB(5, "com.roi.galegot.sequal.filter.single.NAmb"),
	NAMBP(6, "com.roi.galegot.sequal.filter.single.NAmbP"),
	NONIUPAC(7, "com.roi.galegot.sequal.filter.single.NonIupac"),
	PATTERN(8, "com.roi.galegot.sequal.filter.single.Pattern"),
	NOPATTERN(9, "com.roi.galegot.sequal.filter.single.NoPattern"),
	BASEN(10, "com.roi.galegot.sequal.filter.single.BaseN"), BASEP(11, "com.roi.galegot.sequal.filter.single.BaseP"),

	DISTINCT(12, "com.roi.galegot.sequal.filter.group.Distinct"),
	ALMOSTDISTINCT(13, "com.roi.galegot.sequal.filter.group.AlmostDistinct"),
	REVERSEDISTINCT(14, "com.roi.galegot.sequal.filter.group.ReverseDistinct"),
	COMPLEMENTDISTINCT(15, "com.roi.galegot.sequal.filter.group.ComplementDistinct"),
	REVERSECOMPLEMENTDISTINCT(16, "com.roi.galegot.sequal.filter.group.ReverseComplementDistinct");

	public final static int MAX_FILTER_PRIORITY = 16;

	private String filterClassName;
	private int priority;

	private Filters(int priority, String filterClassName) {
		this.priority = priority;
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}

	public int getPriority() {
		return this.priority;
	}
}
