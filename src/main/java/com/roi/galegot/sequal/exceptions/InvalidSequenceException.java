package com.roi.galegot.sequal.exceptions;

@SuppressWarnings("serial")
public class InvalidSequenceException extends RuntimeException {

	public InvalidSequenceException() {
		super("Sequence was malformed or had some sort of error in it.");
	}
}