/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.roundtrip;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.testing.integration.roundtrip.differences.PostgreSqlDumpDiffExpectationsPrepQueue;
import com.databasepreservation.testing.integration.roundtrip.differences.TextDiff;
import com.databasepreservation.testing.integration.roundtrip.differences.TextDiff.Diff;

@Test(groups = {"postgresql-siarddk"})
public class PostgreSqlSIARDDKTest {

  private final String db_source = "dpttestdk";
  private final String db_target = "dpttest_siarddk";
  private final String db_tmp_username = "dpttestdk";
  private final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
  private final String ROUND_TRIP_SIARD_ARCHIVE_FILENAME = "AVID.RND.1000.1";
  private String archiveFullPath;
  private Roundtrip rt;
  protected PostgreSqlDumpDiffExpectationsPrepQueue sqlDumpDiffExpectationsPrepQueue;

  @BeforeClass
  public void setup() throws IOException, InterruptedException, URISyntaxException {
    HashMap<String, String> env_var_source = new HashMap<String, String>();
    env_var_source.put("PGUSER", db_tmp_username);
    env_var_source.put("PGPASSWORD", db_tmp_password);
    env_var_source.put("PGDATABASE", db_source);

    HashMap<String, String> env_var_target = new HashMap<String, String>();
    env_var_target.put("PGUSER", db_tmp_username);
    env_var_target.put("PGPASSWORD", db_tmp_password);
    env_var_target.put("PGDATABASE", db_target);

    Set<PosixFilePermission> executablePermissions = PosixFilePermissions.fromString("rwxr-xr-x");
    Files.setAttribute(Paths.get(getClass().getResource("/postgreSql/scripts/setup.sh").getPath()), "posix:permissions",
      executablePermissions);
    Files.setAttribute(Paths.get(getClass().getResource("/postgreSql/scripts/teardown.sh").getPath()),
      "posix:permissions", executablePermissions);

    archiveFullPath = FileSystems.getDefault()
      .getPath(System.getProperty("java.io.tmpdir"), ROUND_TRIP_SIARD_ARCHIVE_FILENAME).toString();

    sqlDumpDiffExpectationsPrepQueue = new PostgreSqlDumpDiffExpectationsPrepQueue();

    rt = new Roundtrip(
      String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/setup.sh").getPath(),
        db_source, db_target, db_tmp_username, db_tmp_password),
      String.format("%s \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/teardown.sh").getPath(),
        db_source, db_target, db_tmp_username),
      "psql -h 127.0.0.1 --single-transaction -v ON_ERROR_STOP=1 ",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces --exclude-table=tbl_datatypes_prikey_seq",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces --exclude-table=tbl_datatypes_prikey_seq",

      new String[] {"migrate", "--import=postgresql", "--import-hostname=127.0.0.1", "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-dk", "--export-folder", archiveFullPath},

      new String[] {"migrate", "--import=siard-dk-1007", "--import-as-schema=public", "--import-folder", archiveFullPath,
        "--export=postgresql", "--export-hostname=127.0.0.1", "--export-database", db_target, "--export-username",
        db_tmp_username, "--export-password", db_tmp_password, "--export-disable-encryption"},
      sqlDumpDiffExpectationsPrepQueue, env_var_source, env_var_target);
  }

  @Test(description = "PostgreSql server is available and accessible")
  public void testConnection() throws IOException, InterruptedException {
    rt.checkConnection();
  }

  @DataProvider
  public Iterator<Object[]> testQueriesProvider() {
    String singleTypeAndValue = "CREATE SEQUENCE tbl_datatypes_prikey_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;\n"
      + "CREATE TABLE datatypes (col1 %s,col_key integer NOT NULL, CONSTRAINT tbl_datatypes_prikey PRIMARY KEY (col_key) );\n"
      // + "COMMENT ON COLUMN public.datatypes.col1 IS 'Test comment';\n"
      // Awaits: https://github.com/keeps/db-preservation-toolkit/issues/130
      + "INSERT INTO datatypes (col_key,col1) VALUES (nextval('tbl_datatypes_prikey_seq'::regclass),%s);";
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    tests.add(new String[] {singleTypeAndValue, "\"char\" NOT NULL", "'a'"});
    tests.add(new String[] {singleTypeAndValue, "bigint", "123"});
    tests.add(new String[] {singleTypeAndValue, "boolean", "TRUE"});

    tests.add(new String[] {singleTypeAndValue, "character(1)", "'a'"});
    tests.add(new String[] {singleTypeAndValue, "character varying", "'abc'"});
    tests.add(new String[] {singleTypeAndValue, "date", "'2015-01-01'"});
    tests.add(new String[] {singleTypeAndValue, "double precision", "0.123456789012345"});
    tests.add(new String[] {singleTypeAndValue, "integer", "2147483647"});
    tests.add(new String[] {singleTypeAndValue, "name", "'abc'"});
    tests.add(new String[] {singleTypeAndValue, "numeric", "2147483647"});
    tests.add(new String[] {singleTypeAndValue, "real", "0.123456"});
    tests.add(new String[] {singleTypeAndValue, "smallint", "32767"});

    tests.add(new String[] {singleTypeAndValue, "time with time zone", "'23:59:59.999 PST'"});
    tests.add(new String[] {singleTypeAndValue, "time with time zone", "'23:59:59.999+05:30'"});

    return tests.iterator();
  }

  @Test(description = "Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {
    "testConnection"})
  public void testQueries(String... args) throws IOException, InterruptedException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);
    sqlDumpDiffExpectationsPrepQueue.setExpectedDiffs(null);
    assert rt.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);
  }

  @DataProvider
  public Iterator<Object[]> testQueriesWithDiffsProvider() {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();
    String singleTypeAndValue = "CREATE SEQUENCE tbl_datatypes_prikey_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;\n"
      + "CREATE TABLE datatypes (col1 %s,col_key integer NOT NULL, CONSTRAINT tbl_datatypes_prikey PRIMARY KEY (col_key) );\n"
      + "INSERT INTO datatypes (col_key,col1) VALUES (nextval('tbl_datatypes_prikey_seq'::regclass),%s);";
    TextDiff textDiff = new TextDiff();
    tests.add(
      new Object[] {singleTypeAndValue, "text", "'abc'", textDiff.diff_main("text", "character varying(3)", false)});

    tests.add(new Object[] {singleTypeAndValue, "bytea", "NULL", textDiff.diff_main("bytea", "integer", false)});

    return tests.iterator();
  }

  @SuppressWarnings("unchecked")
  @Test(description = "Tests small examples", dataProvider = "testQueriesWithDiffsProvider", dependsOnMethods = {
    "testConnection"})
  public void testQueriesWithDiffs(Object... args) throws IOException, InterruptedException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 2);
    sqlDumpDiffExpectationsPrepQueue.setExpectedDiffs((LinkedList<Diff>) args[args.length - 1]);
    assert rt.testTypeAndValue((String) args[0], fields) : "Query failed: "
      + String.format((String) args[0], (Object[]) fields);
  }

  @DataProvider
  public Iterator<Object[]> testFilesProvider() throws URISyntaxException {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes_udt.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes_with_arrays.sql").toURI())});

    return tests.iterator();
  }

  @Test(description = "Tests PostgreSQL files", dataProvider = "testFilesProvider", dependsOnMethods = {
    "testConnection"})
  public void testFiles(Path... file) throws IOException, InterruptedException, URISyntaxException {
    sqlDumpDiffExpectationsPrepQueue.setExpectedDiffs(null);
    assert rt.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }

}
