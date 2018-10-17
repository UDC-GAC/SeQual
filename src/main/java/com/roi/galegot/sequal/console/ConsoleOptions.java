package com.roi.galegot.sequal.console;

public enum ConsoleOptions {
	GENERATECONFIGFILE("-g"), 
	FILTER("-f"), 
	INPUT("-i"), 
	DOUBLEINPUT("-di"), 
	OUTPUT("-o"), 
	OUTPUTSTATS("-os"),
	CONFIGFILE("-c"), 
	TRIMM("-t"), 
	MEASURE("-s"), 
	FORMAT("-fo"),

	SPARKMASTERCONF("-sc");

	private String opt;

	ConsoleOptions(String opt) {
		this.opt = opt;
	}

	public String getOpt() {
		return this.opt;
	}
}
