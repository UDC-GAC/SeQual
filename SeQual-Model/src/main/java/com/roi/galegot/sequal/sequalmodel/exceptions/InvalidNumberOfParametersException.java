package com.roi.galegot.sequal.sequalmodel.exceptions;

@SuppressWarnings("serial")
public class InvalidNumberOfParametersException extends Exception {

	public InvalidNumberOfParametersException() {
		super("Number of parameters does not match with specified options.");
	}
}