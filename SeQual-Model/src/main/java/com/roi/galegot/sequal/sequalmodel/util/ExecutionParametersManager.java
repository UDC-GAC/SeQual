package com.roi.galegot.sequal.sequalmodel.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The Class ExecutionParametersManager.
 */
public final class ExecutionParametersManager {

	private static String DEFAULT_CONFIGURATION_FILE = "/ExecutionParameters.properties";
	private static String CONFIGURATION_FILE = DEFAULT_CONFIGURATION_FILE;
	private static Map<String, String> parameters;
	private static Boolean modified = false;

	/**
	 * Instantiates a new execution parameters manager.
	 */
	private ExecutionParametersManager() {
	}

	/**
	 * Reads all the parameters from the specified CONFIGURATION_FILE and stores
	 * them in a HashMap.
	 *
	 * @return Map<String, String> pair of Name - Value of the parameters
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static synchronized Map<String, String> getParameters() {

		if (parameters == null) {
			Class<ExecutionParametersManager> configurationParametersManagerClass = ExecutionParametersManager.class;
			ClassLoader classLoader = configurationParametersManagerClass.getClassLoader();
			InputStream inputStream;

			if (modified) {
				try {
					inputStream = new FileInputStream(CONFIGURATION_FILE);
				} catch (FileNotFoundException e1) {
					throw new RuntimeException(e1);
				}
			} else {
				inputStream = classLoader.getResourceAsStream(CONFIGURATION_FILE);
			}
			if (inputStream != null) {
				Properties properties = new Properties();
				try {
					properties.load(inputStream);
					inputStream.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				parameters = new HashMap(properties);
			} else {
				parameters = new HashMap<String, String>();
			}

		}
		return parameters;

	}

	/**
	 * Returns the value of the specified parameter.
	 *
	 * @param param specified parameter
	 * @return String value of the parameter
	 */
	public static String getParameter(String param) {
		return getParameters().get(param);
	}

	/**
	 * Sets a value for the specified parameter.
	 *
	 * @param param specified parameter
	 * @param value specified value for the parameter
	 */
	public static void setParameter(String param, String value) {
		getParameters().put(param, value);
	}

	/**
	 * Sets the file.
	 *
	 * @param path the new file
	 */
	private static synchronized void setFile(String path) {
		CONFIGURATION_FILE = path;
		modified = true;
	}

	/**
	 * Sets the config file.
	 *
	 * @param path the new config file
	 */
	public static void setConfigFile(String path) {
		if (new File(path).exists()) {
			setFile(path);
		} else {
			throw new RuntimeException("Invalid config file");
		}
	}

}