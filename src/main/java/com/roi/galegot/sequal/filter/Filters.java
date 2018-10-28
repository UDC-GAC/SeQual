package com.roi.galegot.sequal.filter;

public enum Filters {
	QUALITYSCORE("com.roi.galegot.sequal.filter.single.QualityScore");

	private String filterClassName;

	private Filters(String filterClassName) {
		this.filterClassName = filterClassName;
	}

	public String getFilterClass() {
		return this.filterClassName;
	}
}
