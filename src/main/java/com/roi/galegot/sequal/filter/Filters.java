package com.roi.galegot.sequal.filter;

public enum Filters {
	PATTERN("com.roi.galegot.sequal.filter.single.Pattern");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
