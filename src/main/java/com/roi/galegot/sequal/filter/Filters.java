package com.roi.galegot.sequal.filter;

public enum Filters {
	GCP("com.roi.galegot.sequal.filter.single.GCP");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
