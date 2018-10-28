package com.roi.galegot.sequal.filter;

public enum Filters {
	NAMB("com.roi.galegot.sequal.filter.single.NAmbP");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
