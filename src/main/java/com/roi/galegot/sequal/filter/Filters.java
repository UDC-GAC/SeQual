package com.roi.galegot.sequal.filter;

public enum Filters {
	BASEN("com.roi.galegot.sequal.filter.single.BaseN");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
