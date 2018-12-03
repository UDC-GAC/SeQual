package com.roi.galegot.sequal.trim.trimmer;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrimmersTest {
	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("SeQual").setMaster("local[*]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void trimLeft() {

	}

	@Test
	public void trimRight() {

	}

	@Test
	public void trimLeftP() {

	}

	@Test
	public void trimRightP() {

	}

	@Test
	public void trimQualLeft() {

	}

	@Test
	public void trimQualRight() {

	}

	@Test
	public void trimLeftToLength() {

	}

	@Test
	public void trimRightToLength() {

	}
}