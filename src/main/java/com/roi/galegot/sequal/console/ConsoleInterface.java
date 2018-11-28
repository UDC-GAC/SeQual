package com.roi.galegot.sequal.console;

import java.io.IOException;
import java.util.Arrays;

import com.roi.galegot.sequal.service.AppService;

/**
 * The Class ConsoleInterface.
 */
public class ConsoleInterface {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {
		int position;
		int lengthArgs;

		String output;
		String input;
		String configFile;
		String masterConf;

		AppService service;

		lengthArgs = args.length;

		if (lengthArgs == 0) {
			instructions();
			return;
		}

		position = findOption(args, ConsoleOptions.OUTPUT.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			System.out.println("Output path not specified correctly.\n");
			instructions();
			return;
		}
		output = args[position + 1];

		position = findOption(args, ConsoleOptions.GENERATECONFIGFILE.getOpt());
		if (position != -1) {
			if (lengthArgs == 3) {
				new AppService(output).generateConfigFile();
				return;
			} else {
				System.out.println("Incorrect number of parameters.\n");
				instructions();
				return;
			}
		}

		if (lengthArgs < 6) {
			System.out.println("Incorrect number of parameters.\n");
			instructions();
			return;
		}

		position = findOption(args, ConsoleOptions.INPUT.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			System.out.println("Input file not specified correctly.\n");
			instructions();
			return;
		}
		input = args[position + 1];

		position = findOption(args, ConsoleOptions.CONFIGFILE.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			System.out.println("Execution parameters file not specified correctly.\n");
			instructions();
			return;
		}
		configFile = args[position + 1];

		position = findOption(args, ConsoleOptions.SPARKMASTERCONF.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			System.out.println("Spark master not specified. All local cores will be used.\n");
			masterConf = "local[*]";
		} else {
			masterConf = args[position + 1];
		}

		service = new AppService(masterConf, input, output, configFile);
		service.read();

		if (findOption(args, ConsoleOptions.FILTER.getOpt()) != -1) {
			service.filter();
		}

		if (findOption(args, ConsoleOptions.FORMAT.getOpt()) != -1) {
			service.format();
		}

		service.write();
		service.end();

		return;

	}

	/**
	 * Find option.
	 *
	 * @param args the args
	 * @param opt  the opt
	 * @return the int
	 */
	private static int findOption(String[] args, String opt) {
		int pos = Arrays.asList(args).indexOf(opt);
		return pos;
	}

	/**
	 * Instructions.
	 */
	private static void instructions() {
		System.out.println("Available options:");
		System.out.println("-o OuputDirectory: Specifies output directory where resulting sequences will be written");
		System.out.println("-i InputFile: Specifies input file from where sequences will be read");
		System.out.println("-c ConfigFile: Specifies run options configuration file");
		System.out.println("-g: Generates a blank configuration file in the specified with -o location");
		System.out.println("-f: Filters read sequences following specified parameters");
		System.out.println("-sc SparkMasterConf: Specifies master configuration (local[*] by default)");
	}
}