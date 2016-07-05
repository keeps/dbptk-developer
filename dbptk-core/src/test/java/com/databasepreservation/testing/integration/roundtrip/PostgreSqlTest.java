package com.databasepreservation.testing.integration.roundtrip;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.testing.integration.roundtrip.differences.PostgreSqlDumpDiffExpectations;

@Test
public class PostgreSqlTest {
  private final String db_source = "dpttest";
  private final String db_target = "dpttest_siard";
  private final String db_tmp_username = "dpttest";
  private final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
  private File tmpFile;
  private boolean needsSetup = true;
  private Roundtrip rt_siard1;
  private Roundtrip rt_siard2;
  private Roundtrip rt_siard2ex;

  @Test(description = "Testing environment setup", groups = {"postgresql-siard1", "postgresql-siard2"})
  public void setup() throws IOException, InterruptedException, URISyntaxException {
    // avoid running a second time
    if (!needsSetup) {
      return;
    }
    needsSetup = false;

    HashMap<String, String> env_var_source = new HashMap<String, String>();
    env_var_source.put("PGUSER", db_tmp_username);
    env_var_source.put("PGPASSWORD", db_tmp_password);
    env_var_source.put("PGDATABASE", db_source);

    HashMap<String, String> env_var_target = new HashMap<String, String>();
    env_var_target.put("PGUSER", db_tmp_username);
    env_var_target.put("PGPASSWORD", db_tmp_password);
    env_var_target.put("PGDATABASE", db_target);

    Set<PosixFilePermission> executablePermissions = PosixFilePermissions.fromString("rwxr-xr-x");
    Files.setAttribute(Paths.get(getClass().getResource("/postgreSql/scripts/setup.sh").getPath()),
      "posix:permissions", executablePermissions);
    Files.setAttribute(Paths.get(getClass().getResource("/postgreSql/scripts/teardown.sh").getPath()),
      "posix:permissions", executablePermissions);

    // Files.setAttribute(p, attribute, value, options)

    rt_siard1 = new Roundtrip(
      String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/setup.sh").getPath(),
        db_source, db_target, db_tmp_username, db_tmp_password),
      String.format("%s \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/teardown.sh").getPath(),
        db_source, db_target, db_tmp_username),
      "psql -q -h 127.0.0.1",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

      new String[] {"--import=postgresql", "--import-hostname=127.0.0.1", "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-1", "--export-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export-pretty-xml"},

      new String[] {"--import=siard-1", "--import-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export=postgresql",
        "--export-hostname=127.0.0.1", "--export-database", db_target, "--export-username", db_tmp_username,
        "--export-password", db_tmp_password, "--export-disable-encryption"}, new PostgreSqlDumpDiffExpectations(),
      env_var_source, env_var_target);

    rt_siard2 = new Roundtrip(
      String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/setup.sh").getPath(),
        db_source, db_target, db_tmp_username, db_tmp_password),
      String.format("%s \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/teardown.sh").getPath(),
        db_source, db_target, db_tmp_username),
      "psql -q -h 127.0.0.1",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

      new String[] {"--import=postgresql", "--import-hostname=127.0.0.1", "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-2", "--export-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export-pretty-xml"},

      new String[] {"--import=siard-2", "--import-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export=postgresql",
        "--export-hostname=127.0.0.1", "--export-database", db_target, "--export-username", db_tmp_username,
        "--export-password", db_tmp_password, "--export-disable-encryption"}, new PostgreSqlDumpDiffExpectations(),
      env_var_source, env_var_target);

    rt_siard2ex = new Roundtrip(
      String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/setup.sh").getPath(),
        db_source, db_target, db_tmp_username, db_tmp_password),
      String.format("%s \"%s\" \"%s\" \"%s\"", getClass().getResource("/postgreSql/scripts/teardown.sh").getPath(),
        db_source, db_target, db_tmp_username),
      "psql -q -h 127.0.0.1",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
      "pg_dump -h 127.0.0.1 --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

      new String[] {"--import=postgresql", "--import-hostname=127.0.0.1", "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-2", "--export-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export-pretty-xml",
        "--export-external-lobs"},

      new String[] {"--import=siard-2", "--import-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export=postgresql",
        "--export-hostname=127.0.0.1", "--export-database", db_target, "--export-username", db_tmp_username,
        "--export-password", db_tmp_password, "--export-disable-encryption"}, new PostgreSqlDumpDiffExpectations(),
      env_var_source, env_var_target);
  }

  @Test(description = "[siard-1] PostgreSql server is available and accessible", groups = {"postgresql-siard1"}, dependsOnMethods = {"setup"})
  public void testConnectionSiard1() throws IOException, InterruptedException {
    rt_siard1.checkConnection();
  }

  @Test(description = "[siard-2] PostgreSql server is available and accessible", groups = {"postgresql-siard2"}, dependsOnMethods = {"setup"})
  public void testConnectionSiard2() throws IOException, InterruptedException {
    rt_siard2.checkConnection();
  }

  @DataProvider
  public Iterator<Object[]> testQueriesProvider() {
    String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    tests.add(new String[] {singleTypeAndValue, "\"char\" NOT NULL", "'a'"});
    tests.add(new String[] {singleTypeAndValue, "bigint", "123"});
    tests.add(new String[] {singleTypeAndValue, "boolean", "TRUE"});
    tests.add(new String[] {singleTypeAndValue, "bytea", "(decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'))"});
    tests.add(new String[] {singleTypeAndValue, "character(1)", "'a'"});
    tests.add(new String[] {singleTypeAndValue, "character varying", "'abc'"});
    tests.add(new String[] {singleTypeAndValue, "date", "'2015-01-01'"});
    tests.add(new String[] {singleTypeAndValue, "double precision", "0.123456789012345"});
    tests.add(new String[] {singleTypeAndValue, "integer", "2147483647"});
    tests.add(new String[] {singleTypeAndValue, "name", "'abc'"});
    tests.add(new String[] {singleTypeAndValue, "numeric", "2147483647"});
    tests.add(new String[] {singleTypeAndValue, "real", "0.123456"});
    tests.add(new String[] {singleTypeAndValue, "smallint", "32767"});
    tests.add(new String[] {singleTypeAndValue, "text", "'abc'"});
    tests.add(new String[] {singleTypeAndValue, "time with time zone", "'23:59:59.999 PST'"});
    tests.add(new String[] {singleTypeAndValue, "time with time zone", "'23:59:59.999+05:30'"});
    // tests.add(new String[]{singleTypeAndValue, "time without time zone",
    // "'23:59:59.999'"});
    // tests.add(new String[]{singleTypeAndValue, "timestamp with time zone",
    // "'2015-01-01 23:59:59.999+8'"});
    // tests.add(new String[]{singleTypeAndValue, "timestamp without time zone",
    // "'2015-01-01 23:59:59.999'"});

    return tests.iterator();
  }

  @Test(description = "[siard-1] Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {"testConnectionSiard1"}, groups = {"postgresql-siard1"})
  public void testQueriesSiard1(String... args) throws IOException, InterruptedException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    assert rt_siard1.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);
  }

  @Test(description = "[siard-2] Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {"testConnectionSiard2"}, groups = {"postgresql-siard2"})
  public void testQueriesSiard2(String... args) throws IOException, InterruptedException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    assert rt_siard2.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);
  }

  @Test(description = "[siard-2-ex] Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {"testConnectionSiard2"}, groups = {"postgresql-siard2"})
  public void testQueriesSiard2ex(String... args) throws IOException, InterruptedException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    assert rt_siard2ex.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);
  }

  @DataProvider
  public Iterator<Object[]> lobQueriesProvider() {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    // lots of small lobs with 10 bytes each
    StringBuilder query = new StringBuilder("CREATE TABLE lobs (col1 bytea);");
    for (int i = 0; i < 1000; i++) {
      query.append("\nINSERT INTO lobs(col1) VALUES(decode('");
      query.append(String.format("%020X", i));
      query.append("', 'hex'));");
    }
    tests.add(new String[] {query.toString()});

    return tests.iterator();
  }

  @Test(description = "[siard-2-ex] Tests external lobs specific examples", dataProvider = "lobQueriesProvider", dependsOnMethods = {"testConnectionSiard2"}, groups = {"postgresql-siard2"})
  public void testQueriesSiard2exSpecific(String... args) throws IOException, InterruptedException {
    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    if (args[0].length() <= 1000) {
      assert rt_siard2ex.testTypeAndValue(args[0], fields) : "Query failed: " + args[0];
    } else {
      assert rt_siard2ex.testTypeAndValue(args[0], fields) : "Query failed: " + args[0].substring(0, 1000);
    }
  }

  @DataProvider
  public Iterator<Object[]> testFilesProvider() throws URISyntaxException {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    tests.add(new Path[] {Paths.get(getClass().getResource("/postgreSql/testfiles/schemas.sql").toURI())});
    tests.add(new Path[] {Paths.get(getClass().getResource("/postgreSql/testfiles/case_sensitivity.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes_udt.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes_with_arrays.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/world.sql").toURI())});
    // tests.add(new
    // Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/comments.sql").toURI())});

    return tests.iterator();
  }

  @Test(description = "[siard-1] Tests PostgreSQL files", dataProvider = "testFilesProvider", dependsOnMethods = {"testConnectionSiard1"}, groups = {"postgresql-siard1"})
  public void testFilesSiard1(Path... file) throws IOException, InterruptedException, URISyntaxException {
    assert rt_siard1.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }

  @Test(description = "[siard-2] Tests PostgreSQL files", dataProvider = "testFilesProvider", dependsOnMethods = {"testConnectionSiard2"}, groups = {"postgresql-siard2"})
  public void testFilesSiard2(Path... file) throws IOException, InterruptedException, URISyntaxException {
    assert rt_siard2.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }

  @Test(description = "[siard-2-ex] Tests PostgreSQL files", dataProvider = "testFilesProvider", dependsOnMethods = {"testConnectionSiard2"}, groups = {"postgresql-siard2"})
  public void testFilesSiard2ex(Path... file) throws IOException, InterruptedException, URISyntaxException {
    assert rt_siard2ex.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }
}
