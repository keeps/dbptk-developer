package com.databasepreservation.integration.roundtrip;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

@Test(groups={"mysql-siard1.0"})
public class MySqlTest {
	final String db_source = "dpttest";
	final String db_target = "dpttest_siard";
	final String db_tmp_username = "dpttest";
	final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
	Roundtrip rt;

	@BeforeClass
	public void setup() throws IOException, InterruptedException, URISyntaxException{
		Set<PosixFilePermission> executablePermissions = PosixFilePermissions.fromString("rwxr-xr-x");
		Files.setAttribute(Paths.get(getClass().getResource("/mySql/scripts/setup.sh").getPath()), "posix:permissions", executablePermissions);
		Files.setAttribute(Paths.get(getClass().getResource("/mySql/scripts/teardown.sh").getPath()), "posix:permissions", executablePermissions);

		rt = new Roundtrip(
				String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"",
						getClass().getResource("/mySql/scripts/setup.sh").getPath(),
						db_source, db_target, db_tmp_username, db_tmp_password),
				String.format("%s \"%s\" \"%s\" \"%s\"",
						getClass().getResource("/mySql/scripts/teardown.sh").getPath(),
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

	@Test(description="MySql server is available and accessible")
	public void testConnection() throws IOException, InterruptedException{
		rt.checkConnection();
	}

	@DataProvider
	public Iterator<Object[]> testQueriesProvider() {
		String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";
		ArrayList<Object[]> tests = new ArrayList<Object[]>();

		//tests.add(new String[]{singleTypeAndValue, "TINYINT", "1"});
		tests.add(new String[]{singleTypeAndValue, "SMALLINT", "123"});
		//tests.add(new String[]{singleTypeAndValue, "MEDIUMINT", "123"});
		tests.add(new String[]{singleTypeAndValue, "INT", "123"});
		//tests.add(new String[]{singleTypeAndValue, "BIGINT", "123"});
		tests.add(new String[]{singleTypeAndValue, "DECIMAL", "123"});
		tests.add(new String[]{singleTypeAndValue, "NUMERIC", "123"});
		tests.add(new String[]{singleTypeAndValue, "FLOAT", "12345.123"});
		tests.add(new String[]{singleTypeAndValue, "FLOAT(12)", "12345.123"});
		//tests.add(new String[]{singleTypeAndValue, "FLOAT(12,0)", "12345.123"});
		//tests.add(new String[]{singleTypeAndValue, "FLOAT(8,3)", "12345.123"});
		tests.add(new String[]{singleTypeAndValue, "DOUBLE", "1234567890.12345"});
		tests.add(new String[]{singleTypeAndValue, "BIT(1)", "b'1'"});
		tests.add(new String[]{singleTypeAndValue, "BIT(64)", "b'10101010101010101010101010101010101010101010101010101010101'"});
		tests.add(new String[]{singleTypeAndValue, "DATE", "'9999-12-31'"});
		tests.add(new String[]{singleTypeAndValue, "DATETIME", "'9999-12-31 23:59:59.999999'"});
		tests.add(new String[]{singleTypeAndValue, "TIMESTAMP", "'2038-01-19 03:14:07.999999'"});
		//tests.add(new String[]{singleTypeAndValue, "YEAR(4)", "'2015'"});
		//tests.add(new String[]{singleTypeAndValue, "CHAR(255)", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "VARCHAR(1024)", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "BINARY(255)", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "VARBINARY(1024)", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "TINYBLOB", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "BLOB", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "MEDIUMBLOB", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "LONGBLOB", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "TINYTEXT", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "TEXT", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "MEDIUMTEXT", "NULL"});
		tests.add(new String[]{singleTypeAndValue, "LONGTEXT", "NULL"});
		//tests.add(new String[]{singleTypeAndValue, "ENUM('small','medium','large')", "NULL"});
		//tests.add(new String[]{singleTypeAndValue, "SET('one','two','three')", "NULL"});

		return tests.iterator();
	}

	@Test(description="Tests small examples", dataProvider="testQueriesProvider",dependsOnMethods={"testConnection"})
	public void testQueries(String... args) throws IOException, InterruptedException{

		String[] fields = new String[args.length-1];
		System.arraycopy(args, 1, fields, 0, args.length-1);

		assert rt.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], fields);
	}

	@DataProvider
	public Iterator<Object[]> testFilesProvider() throws URISyntaxException {
		ArrayList<Object[]> tests = new ArrayList<Object[]>();

		//tests.add(new Path[]{Paths.get(getClass().getResource("/mySql/testfiles/datatypes.sql").toURI())});

		return tests.iterator();
	}

	@Test(description="Tests MySQL files", dataProvider="testFilesProvider",dependsOnMethods={"testConnection"})
	public void testFiles(Path... file) throws IOException, InterruptedException, URISyntaxException{
		assert rt.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
	}
}
