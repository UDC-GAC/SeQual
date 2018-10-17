package com.roi.galegot.sequal.filter;

public enum Filters {
	EXAMPLE("EXAMPLE");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
