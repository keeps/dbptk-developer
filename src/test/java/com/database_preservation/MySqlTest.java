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
		System.out.println("setup complete for mysql-siard1.0");
	}

	@Test(description="Tests small examples", groups={"mysql-siard1.0"})
	public void testQueries() throws IOException, InterruptedException{
		String format ="CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";

		assert rt.testTypeAndValue(format, "TINYINT", "1") : "TINYINT failed";
		assert rt.testTypeAndValue(format, "SMALLINT", "1") : "SMALLINT failed";
		assert rt.testTypeAndValue(format, "MEDIUMINT", "1") : "SMALLINT failed";
	}

	@Test(description="Tests MySQL files in src/test/resources", groups={"mysql-siard1.0"})
	public void testFiles() throws IOException, InterruptedException, URISyntaxException{
		assert rt.testFile(Paths.get(getClass().getResource("/mysql_1.sql").toURI()).toFile()) : "Failed to convert file mysql_1.sql";
	}
}
