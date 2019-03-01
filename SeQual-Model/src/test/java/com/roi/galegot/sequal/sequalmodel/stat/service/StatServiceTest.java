package com.roi.galegot.sequal.sequalmodel.stat.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.service.StatService;
import com.roi.galegot.sequal.sequalmodel.stat.StatsPhrasing;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class StatServiceTest {

	private StatService statService;

	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("SeQual").setMaster("local[*]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@Before
	public void setupService() {
		this.statService = new StatService();
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void statService() {
		/*
		 * Sequence 1 Quality = 14.566666666666666
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Sequence 2 Quality = 30.103448275862068
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Sequence 3 Quality = 20.964285714285715
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original1 = jsc
				.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> original2 = jsc
				.parallelize(Arrays.asList(seq1, seq2));

		Map<String, Double> results;
		String resultsString;

		assertTrue(this.statService.getResults().isEmpty());

		ExecutionParametersManager.setParameter("Statistics", "");
		this.statService.measure(original1, true);
		assertTrue(this.statService.getResults().isEmpty());
		resultsString = "";
		assertEquals(resultsString, this.statService.getResultsAsString());

		this.statService.measure(original1, false);
		assertTrue(this.statService.getResults().isEmpty());
		resultsString = "";
		assertEquals(resultsString, this.statService.getResultsAsString());

		ExecutionParametersManager.setParameter("Statistics", "COUNT");
		this.statService.measure(original1, true);
		results = this.statService.getResults();
		assertFalse(results.isEmpty());
		assertTrue(results.get(StatsPhrasing.COUNT_BEFORE) == 3);
		assertTrue(results.get(StatsPhrasing.COUNT_AFTER) == null);
		resultsString = "Count before transformations: 3.0\n";
		assertEquals(resultsString, this.statService.getResultsAsString());

		this.statService.measure(original2, false);
		results = this.statService.getResults();
		assertFalse(results.isEmpty());
		assertTrue(results.get(StatsPhrasing.COUNT_BEFORE) == 3);
		assertTrue(results.get(StatsPhrasing.COUNT_AFTER) == 2);
		resultsString = "Count before transformations: 3.0\n"
				+ "Count after transformations: 2.0\n";
		assertEquals(resultsString, this.statService.getResultsAsString());

		ExecutionParametersManager.setParameter("Statistics",
				"COUNT|MEANLENGTH|MEANQUALITY");
		this.statService.measure(original1, true);
		results = this.statService.getResults();
		assertFalse(results.isEmpty());
		assertTrue(results.get(StatsPhrasing.COUNT_BEFORE) == 3);
		assertTrue(results.get(StatsPhrasing.COUNT_AFTER) == null);
		assertTrue(results.get(StatsPhrasing.MEAN_LENGTH_BEFORE) == 29);
		assertTrue(results.get(StatsPhrasing.MEAN_LENGTH_AFTER) == null);
		resultsString = "Count before transformations: 3.0\n"
				+ "Mean quality before transformations: 21.88\n"
				+ "Mean length before transformations: 29.0\n";
		assertEquals(resultsString, this.statService.getResultsAsString());

		this.statService.measure(original2, false);
		results = this.statService.getResults();
		assertFalse(results.isEmpty());
		assertTrue(results.get(StatsPhrasing.COUNT_BEFORE) == 3);
		assertTrue(results.get(StatsPhrasing.COUNT_AFTER) == 2);
		assertTrue(results.get(StatsPhrasing.MEAN_LENGTH_BEFORE) == 29);
		assertTrue(results.get(StatsPhrasing.MEAN_LENGTH_AFTER) == 29.5);
		resultsString = "Count before transformations: 3.0\n"
				+ "Mean quality before transformations: 21.88\n"
				+ "Mean length before transformations: 29.0\n"
				+ "Count after transformations: 2.0\n"
				+ "Mean quality after transformations: 22.34\n"
				+ "Mean length after transformations: 29.5\n";
		assertEquals(resultsString, this.statService.getResultsAsString());

	}
}
