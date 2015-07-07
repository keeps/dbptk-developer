package com.database_preservation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Scanner;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.*;

import com.database_preservation.diff_match_patch.Diff;

public class MySqlTest {
	File tmpFile;

	final String db_source = "dpttest";
	final String db_target = "dpttest_siard";
	final String db_tmp_username = "dpttest";
	final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);

	@BeforeGroups(groups={"mysql-siard1.0"})
	public void setUp() throws IOException, InterruptedException{
		// clean up before setting up
		ProcessBuilder teardown = new ProcessBuilder("bash", "-c",
				String.format("./testing/mysql/teardown.sh \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username));
		teardown.redirectOutput(Redirect.INHERIT);
		teardown.redirectError(Redirect.INHERIT);
		Process p = teardown.start();
		System.out.println("td code: " + p.waitFor());

		// create siard 1.0 zip file
		tmpFile = File.createTempFile("dptsiard", ".zip");

		// create user, database and give permissions to the user
		ProcessBuilder setup = new ProcessBuilder("bash", "-c",
				String.format("./testing/mysql/setup.sh \"%s\" \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username, db_tmp_password));
		setup.redirectOutput(Redirect.INHERIT);
		setup.redirectError(Redirect.INHERIT);
		p = setup.start();
		System.out.println("td code: " + p.waitFor());
	}

	@Test(groups={"mysql-siard1.0"})
	public void atest() throws IOException, InterruptedException, URISyntaxException{
		ProcessBuilder sql = new ProcessBuilder("bash", "-c",
				String.format("mysql --user=\"%s\" --password=\"%s\" --database=\"%s\"",
					db_tmp_username, db_tmp_password, db_source));
		sql.redirectOutput(Redirect.INHERIT);
		sql.redirectError(Redirect.INHERIT);
		sql.redirectInput(
				Paths.get(
						getClass().
						getResource("/mysql_1.sql").
						toURI()
						).toFile());

		Process p = sql.start();
		System.out.println("1td code: " + p.waitFor());


		Path dumpsDir = Files.createTempDirectory("dpttest_dumps");

		File dump_source = new File(dumpsDir.toFile().getAbsoluteFile() + "/source.sql");
		File dump_target = new File(dumpsDir.toFile().getAbsoluteFile() + "/target.sql");

		ProcessBuilder dump = new ProcessBuilder("bash", "-c",
				String.format("mysqldump -v --user=\"%s\" --password=\"%s\" %s --compact",
					db_tmp_username, db_tmp_password, db_source));
		dump.redirectOutput(dump_source);
		dump.redirectError(Redirect.INHERIT);
		//dump.redirectOutput(Redirect.INHERIT);
		p = dump.start();
		System.out.println("2td code: " + p.waitFor());

		Main.main("-i", "MySQLJDBC", "localhost", db_source, db_tmp_username, db_tmp_password, "-o", "SIARD", tmpFile.getAbsolutePath(), "store");
		Main.main("-i", "SIARD", tmpFile.getAbsolutePath(), "-o", "MySQLJDBC", "localhost", db_target, db_tmp_username, db_tmp_password);

		dump = new ProcessBuilder("bash", "-c",
				String.format("mysqldump -v --user=\"%s\" --password=\"%s\" %s --compact",
					db_tmp_username, db_tmp_password, db_target));
		dump.redirectOutput(dump_target);
		dump.redirectError(Redirect.INHERIT);
		//dump.redirectOutput(Redirect.INHERIT);
		p = dump.start();
		System.out.println("3td code: " + p.waitFor());


		Scanner dump_source_reader = new Scanner(dump_source).useDelimiter("\\Z");
		Scanner dump_target_reader = new Scanner(dump_target).useDelimiter("\\Z");

		diff_match_patch diff = new diff_match_patch();
		LinkedList<Diff> diffs = diff.diff_main(
				dump_source_reader.next(),
				dump_target_reader.next()
				);
		dump_source_reader.close();
		dump_target_reader.close();

		//diff.diff_prettySimpleCmd(diffs);

		boolean sameText = true;
		for( Diff aDiff : diffs ){
			if( aDiff.operation != diff_match_patch.Operation.EQUAL ){
				sameText = false;
				break;
			}
		}

		if( !sameText )
			System.out.println(diff.diff_prettyCmd(diffs));

		assert sameText : "MySQL dumps are equal.";
	}

	@AfterGroups(groups={"mysql-siard1.0"})
	public void tearDown() throws IOException{
		tmpFile.delete();

		// clean up script
		ProcessBuilder teardown = new ProcessBuilder("bash", "-c",
				String.format("./testing/mysql/teardown.sh \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username));
		teardown.redirectOutput(Redirect.INHERIT);
		teardown.redirectError(Redirect.INHERIT);
		Process p = teardown.start();
	}
}
