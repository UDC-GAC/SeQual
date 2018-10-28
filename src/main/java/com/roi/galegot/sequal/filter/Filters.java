package com.roi.galegot.sequal.filter;

public enum Filters {
	LENGTH("com.roi.galegot.sequal.filter.single.Length"), QUALITY("com.roi.galegot.sequal.filter.single.Quality"),
	QUALITYSCORE("com.roi.galegot.sequal.filter.single.QualityScore"),
	NAMB("com.roi.galegot.sequal.filter.single.NAmb"), NAMBP("com.roi.galegot.sequal.filter.single.NAmbP"),
	GC("com.roi.galegot.sequal.filter.single.GC"), GCP("com.roi.galegot.sequal.filter.single.GCP"),
	NONIUPAC("com.roi.galegot.sequal.filter.single.NonIupac"), PATTERN("com.roi.galegot.sequal.filter.single.Pattern"),
	NOPATTERN("com.roi.galegot.sequal.filter.single.NoPattern"), BASEN("com.roi.galegot.sequal.filter.single.BaseN"),
	BASEP("com.roi.galegot.sequal.filter.single.BaseP"),

	DISTINCT("com.roi.galegot.sequal.filter.group.Distinct"),
	ALMOSTDISTINCT("com.roi.galegot.sequal.filter.group.AlmostDistinct"),
	REVERSEDISTINCT("com.roi.galegot.sequal.filter.group.ReverseDistinct");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
