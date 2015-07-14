package com.databasepreservation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

import com.databasepreservation.diff_match_patch.Diff;
import com.databasepreservation.utils.FileUtils;

public class Roundtrip {
	public static final String TMP_FILE_SIARD_VAR = "%TMP_FILE_SIARD%";

	private static final Logger logger = Logger.getLogger(Roundtrip.class);

	// set by constructor
	private String setup_command;
	private String teardown_command;
	private String populate_command;
	private String dump_source_command;
	private String dump_target_command;
	private String[] forward_conversion_arguments;
	private String[] backward_conversion_arguments;
	private HashMap<String, String> environment_variables_source; //used in populate step and when dumping source database
	private HashMap<String, String> environment_variables_target; //used when dumping target database

	// set internally at runtime
	private Path tmpFileSIARD;

	private File processSTDERR;
	private File processSTDOUT;

	// constants
	private final String db_source = "dpttest";
	private final String db_target = "dpttest_siard";
	private final String db_tmp_username = "dpttest";
	private final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);

	public Roundtrip(){
		assert false : "Roundtrip() should never be called.";
	}

	public Roundtrip(String setup_command, String teardown_command, String populate_command,
			String dump_source_command, String dump_target_command,
			String[] forward_conversion_arguments, String[] backward_conversion_arguments) throws IOException{
		this.setup_command = setup_command;
		this.populate_command = populate_command;
		this.teardown_command = teardown_command;
		this.dump_source_command = dump_source_command;
		this.dump_target_command = dump_target_command;
		this.forward_conversion_arguments = forward_conversion_arguments;
		this.backward_conversion_arguments = backward_conversion_arguments;
		this.environment_variables_source = new HashMap<String, String>();
		this.environment_variables_target = new HashMap<String, String>();

		processSTDERR = File.createTempFile("processSTDERR_", ".tmp");
		processSTDOUT = File.createTempFile("processSTDOUT_", ".tmp");
		processSTDERR.deleteOnExit();
		processSTDOUT.deleteOnExit();
	}

	public Roundtrip(String setup_command, String teardown_command, String populate_command,
			String dump_source_command, String dump_target_command,
			String[] forward_conversion_arguments, String[] backward_conversion_arguments,
			HashMap<String, String> environment_variables_source,HashMap<String, String> environment_variables_target) throws IOException{
		this(setup_command, teardown_command, populate_command,
				dump_source_command, dump_target_command,
				forward_conversion_arguments, backward_conversion_arguments);
		this.environment_variables_source = environment_variables_source;
		this.environment_variables_target = environment_variables_target;
	}

	/**
	 * Sets up and tears down the roundtrip test environment. Asserting that everything works.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void checkConnection() throws IOException, InterruptedException{
		assert setup() == 0 : "Roundtrip setup exit status was not 0";
		assert teardown() == 0 : "Roundtrip teardown exit status was not 0";
	}

	public boolean testTypeAndValue(String template, String... args) throws IOException, InterruptedException{
		//File populate_file = File.createTempFile("roundtrip_populate", ".sql");

		Path populate_file = Files.createTempFile("roundtrip_populate", ".sql");

		BufferedWriter bw = Files.newBufferedWriter(populate_file, StandardCharsets.UTF_8);

		bw.append(String.format(template, (Object[])args));
		bw.newLine();
		bw.close();

		setup();
		boolean result = roundtrip(populate_file);
		teardown();
		Files.deleteIfExists(populate_file);
		return result;
	}

	public boolean testFile(Path populate_file) throws IOException, InterruptedException{
		assert setup() == 0 : "Roundtrip setup exit status was not 0";
		boolean result = roundtrip(populate_file);
		assert teardown() == 0 : "Roundtrip teardown exit status was not 0";
		return result;
	}

	/**
	 * Runs a roundtrip test
	 *
	 * @param populate_file File with queries to populate the database
	 * @return A diff string if the dumps differ, null otherwise
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private boolean roundtrip(Path populate_file) throws IOException, InterruptedException{
		ProcessBuilder sql = new ProcessBuilder("bash", "-c", populate_command);
		sql.redirectOutput(processSTDOUT);
		sql.redirectError(processSTDERR);
		sql.redirectInput(populate_file.toFile());
		for(Entry<String, String> entry : environment_variables_source.entrySet()) {
		    sql.environment().put(entry.getKey(), entry.getValue());
		}
		Process p = sql.start();
		printTmpFileOnError(processSTDERR, p.waitFor());
		printTmpFileOnError(processSTDOUT, p.waitFor());


		Path dumpsDir = Files.createTempDirectory("dpttest_dumps");

		Path dump_source = dumpsDir.resolve("source.sql");
		Path dump_target = dumpsDir.resolve("target.sql");

		ProcessBuilder dump = new ProcessBuilder("bash", "-c",dump_source_command);
		dump.redirectOutput(dump_source.toFile());
		dump.redirectError(processSTDERR);
		for(Entry<String, String> entry : environment_variables_source.entrySet()) {
		    dump.environment().put(entry.getKey(), entry.getValue());
		}
		p = dump.start();
		printTmpFileOnError(processSTDERR, p.waitFor());

		int mainExitStatus;

		mainExitStatus = Main.internal_main(reviewArguments(forward_conversion_arguments));
		if( mainExitStatus != 0 )
			return false;

		mainExitStatus = Main.internal_main(reviewArguments(backward_conversion_arguments));
		if( mainExitStatus != 0 )
			return false;

		dump = new ProcessBuilder("bash", "-c",dump_target_command);
		dump.redirectOutput(dump_target.toFile());
		dump.redirectError(processSTDERR);
		for(Entry<String, String> entry : environment_variables_target.entrySet()) {
		    dump.environment().put(entry.getKey(), entry.getValue());
		}
		p = dump.start();
		printTmpFileOnError(processSTDERR, p.waitFor());

		diff_match_patch diff = new diff_match_patch();
		LinkedList<Diff> diffs = diff.diff_main(
				new String(Files.readAllBytes(dump_source), StandardCharsets.UTF_8),
				new String(Files.readAllBytes(dump_target), StandardCharsets.UTF_8)
				);

		Files.deleteIfExists(dump_source);
		Files.deleteIfExists(dump_target);

		FileUtils.deleteDirectoryRecursive(dumpsDir);

		for( Diff aDiff : diffs ){
			if( aDiff.operation != diff_match_patch.Operation.EQUAL ){
				logger.error("Dump files differ. Outputting differences");
				System.out.println(diff.diff_prettyCmd(diffs));
				return false;
			}
		}

		return true;
	}

	private int setup() throws IOException, InterruptedException{
		// clean up before setting up
		ProcessBuilder teardown = new ProcessBuilder("bash", "-c", teardown_command);
		teardown.redirectOutput(processSTDOUT);
		teardown.redirectError(processSTDERR);
		Process p = teardown.start();
		printTmpFileOnError(processSTDERR, p.waitFor());
		printTmpFileOnError(processSTDOUT, p.waitFor());

		// create siard 1.0 zip file
		tmpFileSIARD = Files.createTempFile("dptsiard", ".zip");

		// create user, database and give permissions to the user
		ProcessBuilder setup = new ProcessBuilder("bash", "-c", setup_command);
		setup.redirectOutput(processSTDOUT);
		setup.redirectError(processSTDERR);
		p = setup.start();

		printTmpFileOnError(processSTDERR, p.waitFor());
		printTmpFileOnError(processSTDOUT, p.waitFor());
		return p.waitFor();
	}

	private int teardown() throws IOException, InterruptedException{
		Files.deleteIfExists(tmpFileSIARD);

		// clean up script
		ProcessBuilder teardown = new ProcessBuilder("bash", "-c", teardown_command);
		teardown.redirectOutput(processSTDOUT);
		teardown.redirectError(processSTDERR);

		Process p = teardown.start();
		printTmpFileOnError(processSTDERR, p.waitFor());
		printTmpFileOnError(processSTDOUT, p.waitFor());
		return p.waitFor();
	}

	private String[] reviewArguments(String[] args){
		String[] copy = new String[args.length];
		for(int i=0; i < args.length; i++){
			if( args[i].equals(TMP_FILE_SIARD_VAR) )
				copy[i] = tmpFileSIARD.toString();
			else
				copy[i] = args[i];
		}
		return copy;
	}

	private void printTmpFileOnError(File file_to_print, int status_code) throws IOException{
		if( status_code == 0 )
			return;

		logger.error("non-zero exit code, printing process output from " + file_to_print.getName());

		if( file_to_print.length() <= 0L )
			return;

		FileReader fr;
		try {
			fr = new FileReader(file_to_print);
			try {
				BufferedReader br = new BufferedReader(fr);
				String line;
				while ((line = br.readLine()) != null) {
					System.out.println(line);
				}
				br.close();
			} catch (IOException e) {
				logger.error("Could not read file", e);
			} finally {
				fr.close();
			}
		} catch (FileNotFoundException e) {
			logger.error("File not found", e);
		}

	}
}
