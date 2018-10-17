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

		String output, input;
		String masterConf;

		AppService service;

		lengthArgs = args.length;

		if (lengthArgs == 0) {
			instructions();
			return;
		}

		if (lengthArgs < 2) {
			System.out.println("Incorrect number of parameters.\n");
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

		position = findOption(args, ConsoleOptions.INPUT.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			System.out.println("Input file not specified correctly.\n");
			instructions();
			return;
		}
		input = args[position + 1];

		position = findOption(args, ConsoleOptions.SPARKMASTERCONF.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			masterConf = "local[*]";
		} else {
			masterConf = args[position + 1];
		}

		service = new AppService(masterConf, input, output);
		service.read();

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
		System.out.println("-sc SparkMasterConf: Specifies master configuration (local[*] by default)");
	}
}