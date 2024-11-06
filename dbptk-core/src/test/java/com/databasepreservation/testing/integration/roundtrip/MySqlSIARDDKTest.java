/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.roundtrip;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.testing.integration.roundtrip.differences.MySqlDumpDiffExpectations;

@Test(groups = {"mysql-siarddk"})
public class MySqlSIARDDKTest {
  private final String db_source = "dpttestdk";
  private final String db_target = "dpttest_siarddk";
  private final String db_tmp_username = "dpttestdk";
  private final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
  private Roundtrip rt;
  private String archiveFullPath;
  private final String ROUND_TRIP_SIARD_ARCHIVE_FILENAME = "AVID.RND.2000.1";

  @BeforeClass
  public void setup() throws IOException, InterruptedException, URISyntaxException {
    Set<PosixFilePermission> executablePermissions = PosixFilePermissions.fromString("rwxr-xr-x");
    Files.setAttribute(Paths.get(getClass().getResource("/mySql/scripts/setup.sh").getPath()), "posix:permissions",
      executablePermissions);
    Files.setAttribute(Paths.get(getClass().getResource("/mySql/scripts/teardown.sh").getPath()), "posix:permissions",
      executablePermissions);

    archiveFullPath = FileSystems.getDefault()
      .getPath(System.getProperty("java.io.tmpdir"), ROUND_TRIP_SIARD_ARCHIVE_FILENAME).toString();

    rt = new Roundtrip(
      String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"", getClass().getResource("/mySql/scripts/setup.sh").getPath(),
        db_source, db_target, db_tmp_username, db_tmp_password),
      String.format("%s \"%s\" \"%s\" \"%s\"", getClass().getResource("/mySql/scripts/teardown.sh").getPath(),
        db_source, db_target, db_tmp_username),
      String.format("mysql  --user=\"%s\" --password=\"%s\" --database=\"%s\"", db_tmp_username, db_tmp_password,
        db_source),
      String.format("mysqldump -v --user=\"%s\" --password=\"%s\" %s --compact ", db_tmp_username, db_tmp_password,
        db_source),
      String.format("mysqldump -v --user=\"%s\" --password=\"%s\" %s --compact", db_tmp_username, db_tmp_password,
        db_target),

      new String[] {"migrate", "--import=mysql", "--import-hostname=127.0.0.1", "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--export=siard-dk-1007",
        "--export-folder", archiveFullPath},

      new String[] {"migrate", "--import=siard-dk-1007", "--import-as-schema=dpttest", "--import-folder", archiveFullPath,
        "--export=mysql", "--export-hostname=127.0.0.1", "--export-database", db_target, "--export-username",
        db_tmp_username, "--export-password", db_tmp_password},

      new MySqlDumpDiffExpectations(), null, null);
  }

  @Test(description = "MySql server is available and accessible")
  public void testConnection() throws IOException, InterruptedException {
    rt.checkConnection();
  }

  @DataProvider
  public Iterator<Object[]> testQueriesProvider() {
    String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s COMMENT 'Test comment'"
      + ",col_key int(11) NOT NULL AUTO_INCREMENT COMMENT 'Comment Primary Key Column'" + ",PRIMARY KEY (col_key));"
      + "\nINSERT INTO datatypes(col1) VALUES(%s);";
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    // TODO: test NULL
    tests.add(new String[] {singleTypeAndValue, "TINYINT(10)", "1"}); // the
                                                                      // number
                                                                      // inside
                                                                      // parentheses
                                                                      // is the
                                                                      // display
                                                                      // width,
                                                                      // does
                                                                      // not
                                                                      // affect
                                                                      // datatype
                                                                      // size
                                                                      // and is
                                                                      // ignored
    tests.add(new String[] {singleTypeAndValue, "TINYINT", "1"});
    tests.add(new String[] {singleTypeAndValue, "SMALLINT", "123"});
    tests.add(new String[] {singleTypeAndValue, "MEDIUMINT(10)", "123"});
    tests.add(new String[] {singleTypeAndValue, "MEDIUMINT", "123"});
    tests.add(new String[] {singleTypeAndValue, "INT", "123"});
    tests.add(new String[] {singleTypeAndValue, "BIGINT(30)", "-9223372036854775808"});
    tests.add(new String[] {singleTypeAndValue, "BIGINT", "9223372036854775807"});
    tests.add(new String[] {singleTypeAndValue, "DECIMAL", "123"});
    tests.add(new String[] {singleTypeAndValue, "NUMERIC", "123"});
    tests.add(new String[] {singleTypeAndValue, "FLOAT", "12345.123"});
    tests.add(new String[] {singleTypeAndValue, "FLOAT", "123456789012"});
    tests.add(new String[] {singleTypeAndValue, "FLOAT(9)", "12345.123"}); //
    // in
    // // mysql,
    // // this
    // // creates
    // // a
    // // float(12,0)
    tests.add(new String[] {singleTypeAndValue, "FLOAT(12)", "12345.123"});
    // // in
    // // mysql,
    // // this
    // // creates
    // // a
    // // float(12,0)
    tests.add(new String[] {singleTypeAndValue, "FLOAT(12,0)", "12345.123"});
    // // in
    // // mysql,
    // // this
    // // creates
    // // a
    // // float(12,0)
    tests.add(new String[] {singleTypeAndValue, "FLOAT(53)", "12345.123"});
    // // in
    // // mysql,
    // // this
    // // creates
    // // a
    // // double(22,0)
    tests.add(new String[] {singleTypeAndValue, "FLOAT(8,3)", "12345.123"});
    // // in
    // // mysql,
    // // this
    // // creates
    // // a
    // // float(8,3)
    tests.add(new String[] {singleTypeAndValue, "DOUBLE", "1234567890.12345"});
    tests.add(new String[] {singleTypeAndValue, "DOUBLE(22,0)", "1234567890.12345"});
    tests.add(new String[] {singleTypeAndValue, "DOUBLE(10,2)", "12345678.12345"});
    tests.add(new String[] {singleTypeAndValue, "BIT(1)", "b'1'"});
    tests.add(new String[] {singleTypeAndValue, "BIT", "b'1'"});
    // tests.add(new String[] {singleTypeAndValue, "BIT(1)", "b'0'"});
    // tests.add(new String[] {singleTypeAndValue, "BIT(5)", "b'11111'"});
    // tests.add(new String[] {singleTypeAndValue, "BIT(64)", "b'" +
    // StringUtils.repeat("1001", 16) + "'"});
    // tests.add(new String[] {singleTypeAndValue, "DATE", "'9999-12-31'"});
    // tests.add(new String[] {singleTypeAndValue, "DATE", "'2015-01-01'"});
    // tests.add(new String[] {singleTypeAndValue, "DATETIME", "'9999-12-31
    // 23:59:59.999999'"});
    tests.add(new String[] {singleTypeAndValue, "TIMESTAMP", "'2038-01-19 03:14:07.999999'"});
    // tests.add(new String[]{singleTypeAndValue, "YEAR(2)", "'15'"}); //
    // difficult to make the test pass, because the value is changed from 1 to
    // 2015
    // tests.add(new String[]{singleTypeAndValue, "YEAR(2)", "5"}); //difficult
    // to make the test pass, because the value is changed from 15 to 2015
    // tests.add(new String[]{singleTypeAndValue, "YEAR(2)", "2015"}); //
    // difficult to make the test pass, because the value is changed from 15
    // to// 2015
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "2015"});
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "'0'"});// becomes
    // 2000, zero
    // is not
    // allowed as
    // number
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "1"}); // becomes
    // 2001
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "'1'"});
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "99"}); // becomes
    // 1999
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "'99'"});
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "70"}); // becomes
    // 1970
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "'70'"});
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "69"}); // becomes
    // 2069
    tests.add(new String[] {singleTypeAndValue, "YEAR(4)", "'69'"});
    // TODO: tests character sets and collations
    // tests.add(new String[] {singleTypeAndValue, "CHAR(0)", "NULL"});
    // tests.add(new String[]{singleTypeAndValue, "CHAR(0) NOT NULL", "''"});
    // //fixme: for empty strings, the value becomes null
    tests.add(new String[] {singleTypeAndValue, "CHAR(3)", "'abc'"});
    tests.add(new String[] {singleTypeAndValue, "CHAR(253)", "NULL"});
    tests.add(new String[] {singleTypeAndValue, "CHAR(253) NOT NULL", "'" + StringUtils.repeat("asdf", 63) + "'"});
    tests.add(new String[] {singleTypeAndValue, "CHAR(255)", "NULL"});
    tests.add(new String[] {singleTypeAndValue, "CHAR(255) NOT NULL", "'" + StringUtils.repeat("asdf", 63) + "'"});
    tests.add(new String[] {singleTypeAndValue, "CHAR(255) NOT NULL", "''"});
    // //fixme: similar to CHAR(0) NOT NULL
    // tests.add(new String[] {singleTypeAndValue, "VARCHAR(10) NOT NULL",
    // "''"}); //fixme: similar to CHAR(0) NOT NULL
    tests.add(new String[] {singleTypeAndValue, "VARCHAR(1024)", "NULL"});
    tests.add(new String[] {singleTypeAndValue, "VARCHAR(256)", "'" + StringUtils.repeat("asdf", 64) + "'"});
    // tests.add(new String[] {singleTypeAndValue, "VARCHAR(4098)", "'" +
    // StringUtils.repeat("asdfqwertyuighjk", 4096) + "'"}); //fixme: in siard,
    // small strings are strings, longer strings are clobs
    // TODO: more relevant tests for the types below
    // tests.add(new String[]{singleTypeAndValue, "BINARY(255)", "NULL"});
    // tests.add(new String[]{singleTypeAndValue, "VARBINARY(1024)","NULL"});
    // http://stackoverflow.com/questions/6766781/maximum-length-for-mysql-type-text

    // TINYBLOB is not supported in siard-dk
    // tests.add(new String[] {singleTypeAndValue, "TINYBLOB", "NULL"});
    // TODO: TK: FIX blob test when issue is fixed:
    // https://github.com/keeps/db-preservation-toolkit/issues/128
    /*
     * tests.add(new String[] {singleTypeAndValue, "BLOB", "NULL"}); tests.add(new
     * String[] {singleTypeAndValue, "MEDIUMBLOB", "NULL"}); tests.add(new String[]
     * {singleTypeAndValue, "LONGBLOB", "NULL"}); tests.add(new String[]
     * {singleTypeAndValue, "TINYTEXT", "NULL"}); tests.add(new String[]
     * {singleTypeAndValue, "TEXT", "NULL"}); tests.add(new String[]
     * {singleTypeAndValue, "MEDIUMTEXT", "NULL"}); // tests.add(new String[]
     * {singleTypeAndValue, "LONGTEXT", "NULL"}); // tests.add(new
     * String[]{singleTypeAndValue, // "ENUM('small','medium','large')", "NULL"});
     * // tests.add(new String[]{singleTypeAndValue, "SET('one','two','three')", //
     * "NULL"});
     */

    return tests.iterator();
  }

  @Test(description = "Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {
    "testConnection"})
  public void testQueries(String... args) throws IOException, InterruptedException {
    File archFile = new File(archiveFullPath);
    if (archFile.exists()) {
      FileUtils.deleteDirectory(archFile);
    }
    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    assert rt.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);

  }

  @DataProvider
  public Iterator<Object[]> testFilesProvider() throws URISyntaxException {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/mySql/testfiles/datatypes.sql").toURI())});

    return tests.iterator();
  }

  @Test(description = "Tests MySQL files", dataProvider = "testFilesProvider", dependsOnMethods = {"testConnection"})
  public void testFiles(Path... file) throws IOException, InterruptedException, URISyntaxException {
    assert rt.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }
}
