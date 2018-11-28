package com.roi.galegot.sequal.common;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.roi.galegot.sequal.exceptions.InvalidSequenceException;

/**
 * The Class Sequence.
 */
@SuppressWarnings("serial")
public class Sequence implements Serializable {

	/** The name. */
	private String name;

	/** The sequence string. */
	private String sequenceString;

	/** The extra. */
	private String extra;

	/** The quality string. */
	private String qualityString;

	/** The length. */
	private int length;

	/** The gua cyt. */
	private int guaCyt;

	/** The n amb. */
	private int nAmb;

	/** The gua cyt P. */
	private double guaCytP;

	/** The n amb P. */
	private double nAmbP;

	/** The quality. */
	private double quality;

	/** The has quality. */
	private Boolean hasQuality;

	/**
	 * Instantiates a new sequence.
	 *
	 * @param line1 the line 1
	 * @param line2 the line 2
	 * @throws InvalidSequenceException
	 */
	public Sequence(String line1, String line2) {
		if (!this.checkIsWellFormed(line1, line2)) {
			throw new InvalidSequenceException();
		}

		this.name = line1;
		this.sequenceString = line2;
		this.length = this.sequenceString.length();
		this.guaCyt = this.calcGuaCyt();
		this.guaCytP = this.calcGuaCytP();
		this.nAmb = this.calcNAmb();
		this.nAmbP = this.calcNAmbP();
		this.hasQuality = false;
	}

	/**
	 * Instantiates a new sequence.
	 *
	 * @param line1 the line 1
	 * @param line2 the line 2
	 * @param line3 the line 3
	 * @param line4 the line 4
	 * @throws InvalidSequenceException
	 */
	public Sequence(String line1, String line2, String line3, String line4) {
		if (!this.checkIsWellFormed(line1, line2, line3, line4)) {
			throw new InvalidSequenceException();
		}

		this.name = line1;
		this.sequenceString = line2;
		this.extra = line3;
		this.qualityString = line4;
		this.length = this.sequenceString.length();
		this.quality = this.calcQuality();
		this.guaCyt = this.calcGuaCyt();
		this.guaCytP = this.calcGuaCytP();
		this.nAmb = this.calcNAmb();
		this.nAmbP = this.calcNAmbP();
		this.hasQuality = true;
	}

	@Override
	public String toString() {
		if (this.hasQuality) {
			return this.name + "\n" + this.sequenceString + "\n" + this.extra + "\n" + this.qualityString;
		} else {
			return this.name + "\n" + this.sequenceString;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((this.qualityString == null) ? 0 : this.qualityString.hashCode());
		result = (prime * result) + ((this.sequenceString == null) ? 0 : this.sequenceString.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		Sequence other = (Sequence) obj;
		if (this.qualityString == null) {
			if (other.qualityString != null) {
				return false;
			}
		} else if (!this.qualityString.equals(other.qualityString)) {
			return false;
		}
		if (this.sequenceString == null) {
			if (other.sequenceString != null) {
				return false;
			}
		} else if (!this.sequenceString.equals(other.sequenceString)) {
			return false;
		}
		return true;
	}

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Gets the seq.
	 *
	 * @return the seq
	 */
	public String getSequenceString() {
		return this.sequenceString;
	}

	/**
	 * Gets the extra.
	 *
	 * @return the extra
	 */
	public String getExtra() {
		return this.extra;
	}

	/**
	 * Gets the quality string.
	 *
	 * @return the quality string
	 */
	public String getQualityString() {
		return this.qualityString;
	}

	/**
	 * Gets the length.
	 *
	 * @return the length
	 */
	public int getLength() {
		return this.length;
	}

	/**
	 * Gets the quality.
	 *
	 * @return the quality
	 */
	public double getQuality() {
		return this.quality;
	}

	/**
	 * Gets the gua cyt.
	 *
	 * @return the gua cyt
	 */
	public int getGuaCyt() {
		return this.guaCyt;
	}

	/**
	 * Gets the gua cyt P.
	 *
	 * @return the gua cyt P
	 */
	public double getGuaCytP() {
		return this.guaCytP;
	}

	/**
	 * Gets the n amb.
	 *
	 * @return the n amb
	 */
	public int getNAmb() {
		return this.nAmb;
	}

	/**
	 * Gets the n amb P.
	 *
	 * @return the n amb P
	 */
	public double getNAmbP() {
		return this.nAmbP;
	}

	/**
	 * Checks if is has qual.
	 *
	 * @return the boolean
	 */
	public Boolean isHasQual() {
		return this.hasQuality;
	}

	/**
	 * Calc quality.
	 *
	 * @return the double
	 */
	private double calcQuality() {
		double qual = 0;
		for (char c : this.qualityString.toCharArray()) {
			qual = (qual + c) - 33;
		}
		return qual / this.length;
	}

	/**
	 * Calc gua cyt.
	 *
	 * @return the int
	 */
	private int calcGuaCyt() {
		int guaCyt = 0;
		for (char c : this.sequenceString.toCharArray()) {
			if ((c == 'G') || (c == 'C')) {
				guaCyt++;
			}
		}
		return guaCyt;
	}

	/**
	 * Calc gua cyt P.
	 *
	 * @return the double
	 */
	private double calcGuaCytP() {
		return (double) this.guaCyt / this.length;
	}

	/**
	 * Calc N amb.
	 *
	 * @return the int
	 */
	private int calcNAmb() {
		int nAmb = 0;
		for (char c : this.sequenceString.toCharArray()) {
			if (c == 'N') {
				nAmb++;
			}
		}
		return nAmb;
	}

	/**
	 * Calc N amb P.
	 *
	 * @return the double
	 */
	private double calcNAmbP() {
		return (double) this.nAmb / this.length;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the new name
	 */
	public void setName(String name) {
		if (!(this.checkStart(name, "@") || this.checkStart(name, ">"))) {
			throw new InvalidSequenceException();
		}
		this.name = name;
	}

	/**
	 * Sets the extra.
	 *
	 * @param extra the new extra
	 */
	public void setExtra(String extra) {
		this.extra = extra;
	}

	/**
	 * Sets the seq.
	 *
	 * @param seq the new seq
	 */
	public void setSequenceString(String sequenceString) {
		this.sequenceString = sequenceString;
		this.length = sequenceString.length();
		this.guaCyt = this.calcGuaCyt();
		this.guaCytP = this.calcGuaCytP();
		this.nAmb = this.calcNAmb();
		this.nAmbP = this.calcNAmbP();
	}

	/**
	 * Sets the quality string.
	 *
	 * @param qualityS the new quality string
	 */
	public void setQualityString(String qualityS) {
		if (StringUtils.isBlank(qualityS)) {
			this.hasQuality = false;
			this.qualityString = null;
		} else {
			this.qualityString = qualityS;
			this.quality = this.calcQuality();
		}
	}

	private Boolean checkIsWellFormed(String line1, String line2) {
		if (StringUtils.isBlank(line1) || StringUtils.isBlank(line2)) {
			return false;
		}
		if (!this.checkStart(line1, ">")) {
			return false;
		}
		return true;
	}

	private Boolean checkIsWellFormed(String line1, String line2, String line3, String line4) {
		if (StringUtils.isBlank(line1) || StringUtils.isBlank(line2) || StringUtils.isBlank(line3)
				|| StringUtils.isBlank(line4)) {
			return false;
		}
		if (line2.length() != line4.length()) {
			return false;
		}
		if (!this.checkStart(line1, "@")) {
			return false;
		}
		return true;
	}

	private Boolean checkStart(String line, String starter) {
		return line.startsWith(starter);
	}
}