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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.*;

import com.database_preservation.diff_match_patch.Diff;

@Test(groups={"mysql-siard1.0"})
public class MySqlTest {
	final String db_source = "dpttest";
	final String db_target = "dpttest_siard";
	final String db_tmp_username = "dpttest";
	final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
	Roundtrip rt;

	@BeforeClass
	public void setup() throws IOException, InterruptedException, URISyntaxException{
		rt = new Roundtrip(
				String.format("./testing/mysql/setup.sh \"%s\" \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username, db_tmp_password),
				String.format("./testing/mysql/teardown.sh \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username),
				String.format("mysql --user=\"%s\" --password=\"%s\" --database=\"%s\"",
						db_tmp_username, db_tmp_password, db_source),
				String.format("mysqldump -v --user=\"%s\" --password=\"%s\" %s --compact",
						db_tmp_username, db_tmp_password, db_source),
				String.format("mysqldump -v --user=\"%s\" --password=\"%s\" %s --compact",
						db_tmp_username, db_tmp_password, db_target),
				new String[]{"-i", "MySQLJDBC", "localhost", db_source, db_tmp_username, db_tmp_password, "-o", "SIARD", Roundtrip.TMP_FILE_SIARD_VAR, "store"},
				new String[]{"-i", "SIARD", Roundtrip.TMP_FILE_SIARD_VAR, "-o", "MySQLJDBC", "localhost", db_target, db_tmp_username, db_tmp_password});
	}

	@DataProvider
	public Iterator<Object[]> testQueriesProvider() {
		String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";
		ArrayList<Object[]> tests = new ArrayList<Object[]>();

		tests.add(new String[]{singleTypeAndValue, "TINYINT", "1",});
		tests.add(new String[]{singleTypeAndValue, "SMALLINT", "1"});
		tests.add(new String[]{singleTypeAndValue, "MEDIUMINT", "1"});

		return tests.iterator();
	}

	@Test(description="Tests small examples", dataProvider="testQueriesProvider")
	public void testQueries(String... args) throws IOException, InterruptedException{

		String[] fields = new String[args.length-1];
		System.arraycopy(args, 1, fields, 0, args.length-1);

		assert rt.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], fields);
	}

	@DataProvider
	public Iterator<Object[]> testFilesProvider() throws URISyntaxException {
		ArrayList<Object[]> tests = new ArrayList<Object[]>();

		tests.add(new File[]{Paths.get(getClass().getResource("/mysql_1.sql").toURI()).toFile()});

		return tests.iterator();
	}

	@Test(description="Tests MySQL files", dataProvider="testFilesProvider")
	public void testFiles(File... file) throws IOException, InterruptedException, URISyntaxException{
		assert rt.testFile(file[0]) : "Roundtrip failed for file: " + file[0].getAbsolutePath();
	}
}
