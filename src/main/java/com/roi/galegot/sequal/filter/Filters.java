package com.roi.galegot.sequal.filter;

public enum Filters {
	GC("com.roi.galegot.sequal.filter.single.GC");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
