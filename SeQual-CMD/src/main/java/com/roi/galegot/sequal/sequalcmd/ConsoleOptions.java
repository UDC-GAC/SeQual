package com.roi.galegot.sequal.sequalcmd;

/**
 * The Enum ConsoleOptions.
 */
public enum ConsoleOptions {

	/** The generateconfigfile. */
	GENERATECONFIGFILE("-g"),

	/** The filter. */
	FILTER("-f"),

	/** The input. */
	INPUT("-i"),

	/** The doubleinput. */
	DOUBLEINPUT("-di"),

	/** The output. */
	OUTPUT("-o"),

	/** The singlefileoutput. */
	SINGLEFILEOUTPUT("-sfo"),

	/** The outputstats. */
	OUTPUTSTATS("-os"),

	/** The configfile. */
	CONFIGFILE("-c"),

	/** The trim. */
	TRIM("-t"),

	/** The measure. */
	MEASURE("-s"),

	/** The format. */
	FORMAT("-fo"),

	/** The sparkmasterconf. */
	SPARKMASTERCONF("-smc"),

	/** The sparklogconf. */
	SPARKLOGCONF("-slc");

	/** The opt. */
	private String opt;

	/**
	 * Instantiates a new console options.
	 *
	 * @param opt the opt
	 */
	ConsoleOptions(String opt) {
		this.opt = opt;
	}

	/**
	 * Gets the opt.
	 *
	 * @return the opt
	 */
	public String getOpt() {
		return this.opt;
	}
}
