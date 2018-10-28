package com.roi.galegot.sequal.filter;

public enum Filters {
	LENGTH("com.roi.galegot.sequal.filter.single.Length");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
