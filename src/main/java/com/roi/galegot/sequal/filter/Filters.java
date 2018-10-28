package com.roi.galegot.sequal.filter;

public enum Filters {
	QUALITY("com.roi.galegot.sequal.filter.single.Quality");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
