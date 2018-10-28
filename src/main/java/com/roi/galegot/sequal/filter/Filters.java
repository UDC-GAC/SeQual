package com.roi.galegot.sequal.filter;

public enum Filters {
	LENGTH("com.roi.galegot.sequal.filter.single.Length"), QUALITY("com.roi.galegot.sequal.filter.single.Quality"),
	NAMB("com.roi.galegot.sequal.filter.single.NAmb");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
