package com.databasepreservation;

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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.HashMap;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.*;

import com.databasepreservation.diff_match_patch.Diff;

@Test(groups={"postgresql-siard1.0"})
public class PostgreSqlTest {
	File tmpFile;

	final String db_source = "dpttest";
	final String db_target = "dpttest_siard";
	final String db_tmp_username = "dpttest";
	final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);

	Roundtrip rt;

	@BeforeClass
	public void setup() throws IOException, InterruptedException, URISyntaxException{
		HashMap<String, String> env_var_source = new HashMap<String, String>();
		env_var_source.put("PGUSER", db_tmp_username);
		env_var_source.put("PGPASSWORD", db_tmp_password);
		env_var_source.put("PGDATABASE", db_source);

		HashMap<String, String> env_var_target = new HashMap<String, String>();
		env_var_target.put("PGUSER", db_tmp_username);
		env_var_target.put("PGPASSWORD", db_tmp_password);
		env_var_target.put("PGDATABASE", db_target);

		rt = new Roundtrip(
				String.format("./testing/postgresql/setup.sh \"%s\" \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username, db_tmp_password),
				String.format("./testing/postgresql/teardown.sh \"%s\" \"%s\" \"%s\"",
						db_source, db_target, db_tmp_username),
				"psql -q",
				"pg_dump --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
				"pg_dump --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
				new String[]{"-i", "PostgreSQLJDBC", "localhost", db_source, db_tmp_username, db_tmp_password, "false",
					"-o", "SIARD", Roundtrip.TMP_FILE_SIARD_VAR, "store"},
				new String[]{"-i", "SIARD", Roundtrip.TMP_FILE_SIARD_VAR,
					"-o", "PostgreSQLJDBC", "localhost", db_target, db_tmp_username, db_tmp_password, "false"},
				env_var_source,
				env_var_target);
	}

	@DataProvider
	public Iterator<Object[]> testQueriesProvider() {
		String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";
		ArrayList<Object[]> tests = new ArrayList<Object[]>();

		tests.add(new String[]{singleTypeAndValue, "\"char\" NOT NULL", "'a'"});
		tests.add(new String[]{singleTypeAndValue, "bigint", "123"});
		tests.add(new String[]{singleTypeAndValue, "boolean", "TRUE"});
		tests.add(new String[]{singleTypeAndValue, "bytea", "(decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'))"});
		tests.add(new String[]{singleTypeAndValue, "character(1)", "'a'"});
		tests.add(new String[]{singleTypeAndValue, "character varying", "'abc'"});
		tests.add(new String[]{singleTypeAndValue, "date", "'2015-01-01'"});
		tests.add(new String[]{singleTypeAndValue, "double precision", "0.123456789012345"});
		tests.add(new String[]{singleTypeAndValue, "integer", "2147483647"});
		tests.add(new String[]{singleTypeAndValue, "name", "'abc'"});
		tests.add(new String[]{singleTypeAndValue, "numeric", "2147483647"});
		tests.add(new String[]{singleTypeAndValue, "real", "0.123456"});
		tests.add(new String[]{singleTypeAndValue, "smallint", "32767"});
		tests.add(new String[]{singleTypeAndValue, "text", "'abc'"});
//		tests.add(new String[]{singleTypeAndValue, "time with time zone", "'23:59:59.999 PST'"});
//		tests.add(new String[]{singleTypeAndValue, "time without time zone", "'23:59:59.999'"});
//		tests.add(new String[]{singleTypeAndValue, "timestamp with time zone", "'2015-01-01 23:59:59.999+8'"});
//		tests.add(new String[]{singleTypeAndValue, "timestamp without time zone", "'2015-01-01 23:59:59.999'"});

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

		//tests.add(new File[]{Paths.get(getClass().getResource("/postgresql_1.sql").toURI()).toFile()});

		return tests.iterator();
	}

	@Test(description="Tests PostgreSQL files", dataProvider="testFilesProvider")
	public void testFiles(File... file) throws IOException, InterruptedException, URISyntaxException{
		assert rt.testFile(file[0]) : "Roundtrip failed for file: " + file[0].getAbsolutePath();
	}
}
