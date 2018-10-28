package com.roi.galegot.sequal.filter;

public enum Filters {
	NOPATTERN("com.roi.galegot.sequal.filter.single.NoPattern");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
