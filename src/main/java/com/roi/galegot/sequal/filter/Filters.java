package com.roi.galegot.sequal.filter;

public enum Filters {
	BASEP("com.roi.galegot.sequal.filter.single.BaseP");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
