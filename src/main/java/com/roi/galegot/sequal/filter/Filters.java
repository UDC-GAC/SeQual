package com.roi.galegot.sequal.filter;

public enum Filters {
	LENGTH("com.roi.galegot.sequal.filter.single.Length"), QUALITY("com.roi.galegot.sequal.filter.single.Quality"),
	NAMB("com.roi.galegot.sequal.filter.single.NAmb"), NAMBP("com.roi.galegot.sequal.filter.single.NAmbP"),
	GC("com.roi.galegot.sequal.filter.single.GC"), GCP("com.roi.galegot.sequal.filter.single.GCP"),
	NONIUPAC("com.roi.galegot.sequal.filter.single.NonIupac");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
