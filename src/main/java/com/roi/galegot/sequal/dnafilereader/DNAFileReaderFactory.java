package com.roi.galegot.sequal.dnafilereader;

import org.apache.commons.lang3.StringUtils;

/**
 * A factory for creating DNAFileReader objects.
 */
public class DNAFileReaderFactory {

	/**
	 * Instantiates a new DNA file reader factory.
	 */
	private DNAFileReaderFactory() {
	}

	/**
	 * Creates a DNAFileReader based on the extension of the file to be read.
	 *
	 * @param extension the extension
	 * @return DNAFileReader specifically for that kind of file
	 */
	public synchronized static DNAFileReader getReader(String extension) {
		if (StringUtils.isBlank(extension)) {
			throw new RuntimeException("Supported file formats: FASTQ (.fq .fastq) FASTA(.fa .fasta)");
		}
		switch (extension.toLowerCase()) {
		case "fq":
		case "fastq":
			return new FQReader();

		case "fa":
		case "fasta":
			return new FASTAReader();

		default:
			throw new RuntimeException("Supported file formats: FASTQ (.fq .fastq) FASTA(.fa .fasta)");

		}
	}
}