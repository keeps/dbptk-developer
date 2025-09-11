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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.testing.integration.roundtrip.differences.PostgreSqlDumpDiffExpectations;

@Test
public class PostgreSqlTest {
  private final String db_source = "dpttest";
  private final String db_target = "dpttest_siard";
  private String db_tmp_username;
  private String db_tmp_password;
  private File tmpFile;
  private boolean needsSetup = true;
  private Roundtrip rt_siard1;
  private Roundtrip rt_siard2;
  private Roundtrip rt_siard2ex;

  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  @BeforeClass
  public void beforeClass() throws IOException {
    postgres.start();
  }

  @AfterClass
  public void afterClass() {
    postgres.stop();
  }

  @Test(description = "Testing environment setup", groups = {"postgresql-siard1", "postgresql-siard2"})
  public void setup() throws IOException, InterruptedException, URISyntaxException, SQLException {
    // avoid running a second time
    if (!needsSetup) {
      return;
    }
    needsSetup = false;

    postgres.start();

    DBConnectionProvider connectionProvider = new DBConnectionProvider(postgres.getJdbcUrl(), postgres.getUsername(),
      postgres.getPassword());
    db_tmp_username = postgres.getUsername();
    db_tmp_password = postgres.getPassword();

    HashMap<String, String> env_var_source = new HashMap<String, String>();
    env_var_source.put("PGUSER", postgres.getUsername());
    env_var_source.put("PGPASSWORD", postgres.getPassword());
    env_var_source.put("PGDATABASE", db_source);

    HashMap<String, String> env_var_target = new HashMap<String, String>();
    env_var_target.put("PGUSER", postgres.getUsername());
    env_var_target.put("PGPASSWORD", postgres.getPassword());
    env_var_target.put("PGDATABASE", db_target);

    rt_siard1 = new Roundtrip(new String[] {"CREATE DATABASE " + db_source + ";", "CREATE DATABASE " + db_target + ";"},
      new String[] {"DROP DATABASE IF EXISTS " + db_source + ";", "DROP DATABASE IF EXISTS " + db_target + ";"},
      "psql -q -h " + postgres.getHost(),
      "pg_dump -h " + postgres.getHost()
        + " --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
      "pg_dump -h " + postgres.getHost()
        + " --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

      new String[] {"migrate", "--import=postgresql", "--import-hostname=" + postgres.getHost(),
        "--import-port-number=" + postgres.getFirstMappedPort(), "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-1", "--export-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export-pretty-xml"},

      new String[] {"migrate", "--import=siard-1", "--import-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export=postgresql",
        "--export-hostname=" + postgres.getHost(), "--export-port-number=" + postgres.getFirstMappedPort(),
        "--export-database", db_target, "--export-username", db_tmp_username,
        "--export-password", db_tmp_password, "--export-disable-encryption"},
      new PostgreSqlDumpDiffExpectations(), env_var_source, env_var_target, connectionProvider, postgres);

    rt_siard2 = new Roundtrip(new String[] {"CREATE DATABASE " + db_source + ";", "CREATE DATABASE " + db_target + ";"},
      new String[] {"DROP DATABASE IF EXISTS " + db_source + ";", "DROP DATABASE IF EXISTS " + db_target + ";"},
      "psql -q -h " + postgres.getHost(),
      "pg_dump -h " + postgres.getHost()
        + " --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
      "pg_dump -h " + postgres.getHost()
        + " --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

      new String[] {"migrate", "--import=postgresql", "--import-hostname=" + postgres.getHost(),
        "--import-port-number=" + postgres.getFirstMappedPort(), "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-2", "--export-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export-pretty-xml"},

      new String[] {"migrate", "--import=siard-2", "--import-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export=postgresql",
        "--export-hostname=" + postgres.getHost(), "--export-port-number=" + postgres.getFirstMappedPort(),
        "--export-database", db_target, "--export-username", db_tmp_username,
        "--export-password", db_tmp_password, "--export-disable-encryption"},
      new PostgreSqlDumpDiffExpectations(), env_var_source, env_var_target, connectionProvider, postgres);

    rt_siard2ex = new Roundtrip(
      new String[] {"CREATE DATABASE " + db_source + ";", "CREATE DATABASE " + db_target + ";"},
      new String[] {"DROP DATABASE IF EXISTS " + db_source + ";", "DROP DATABASE IF EXISTS " + db_target + ";"},
      "psql -q -h " + postgres.getHost(),
      "pg_dump -h " + postgres.getHost()
        + " --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
      "pg_dump -h " + postgres.getHost()
        + " --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

      new String[] {"migrate", "--import=postgresql", "--import-hostname=" + postgres.getHost(),
        "--import-port-number=" + postgres.getFirstMappedPort(), "--import-database", db_source,
        "--import-username", db_tmp_username, "--import-password", db_tmp_password, "--import-disable-encryption",
        "--export=siard-2", "--export-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export-pretty-xml",
        "--export-external-lobs", "--export-external-lobs-blob-threshold-limit=0",
        "--export-external-lobs-clob-threshold-limit=0"},

      new String[] {"migrate", "--import=siard-2", "--import-file", Roundtrip.TMP_FILE_SIARD_VAR, "--export=postgresql",
        "--export-hostname=" + postgres.getHost(), "--export-port-number=" + postgres.getFirstMappedPort(),
        "--export-database", db_target, "--export-username", db_tmp_username,
        "--export-password", db_tmp_password, "--export-disable-encryption"},
      new PostgreSqlDumpDiffExpectations(), env_var_source, env_var_target, connectionProvider, postgres);
  }

  @DataProvider
  public Iterator<Object[]> testQueriesProvider() {
    String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    tests.add(new String[] {singleTypeAndValue, "BIT VARYING(5)", "B'101'"});
    tests.add(new String[] {singleTypeAndValue, "BIT(5)", "B'01010'"});
    tests.add(new String[] {singleTypeAndValue, "\"char\" NOT NULL", "'a'"});
    tests.add(new String[] {singleTypeAndValue, "bigint", "123"});
    tests.add(new String[] {singleTypeAndValue, "boolean", "TRUE"});
    tests.add(new String[] {singleTypeAndValue, "bytea", "(decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'))"});
    tests.add(new String[] {singleTypeAndValue, "bytea", "(decode('00000000000000000000000000000000', 'hex'))"});
    tests.add(new String[] {singleTypeAndValue, "bytea", "NULL"});
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

  @Test(description = "[siard-1] Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard1"})
  public void testQueriesSiard1(String... args) throws IOException, InterruptedException, SQLException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    assert rt_siard1.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);
  }

  @Test(description = "[siard-2] Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard2"})
  public void testQueriesSiard2(String... args) throws IOException, InterruptedException, SQLException {

    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    assert rt_siard2.testTypeAndValue(args[0], fields) : "Query failed: " + String.format(args[0], (Object[]) fields);
  }

  @Test(description = "[siard-2-ex] Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard2"})
  public void testQueriesSiard2ex(String... args) throws IOException, InterruptedException, SQLException {

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

  @Test(description = "[siard-2-ex] Tests external lobs specific examples", dataProvider = "lobQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard2"})
  public void testQueriesSiard2exSpecific(String... args) throws IOException, InterruptedException, SQLException {
    String[] fields = new String[args.length - 1];
    System.arraycopy(args, 1, fields, 0, args.length - 1);

    if (args[0].length() <= 1000) {
      assert rt_siard2ex.testTypeAndValue(args[0], fields) : "Query failed: " + args[0];
    } else {
      assert rt_siard2ex.testTypeAndValue(args[0], fields) : "Query failed: " + args[0].substring(0, 1000);
    }
  }

  @DataProvider
  public Iterator<Object> testWorkflowQueriesProvider() throws URISyntaxException {
    ArrayList<Object> tests = new ArrayList<>();

    // schemas.sql
    tests.add("""
      CREATE TABLE public.fstTable (col1 integer);
      INSERT INTO public.fstTable (col1) VALUES (1);
      INSERT INTO public.fstTable (col1) VALUES (2);

      CREATE SCHEMA otherSchema;
      CREATE TABLE otherSchema.fstTable (col1 integer);
      INSERT INTO otherSchema.fstTable (col1) VALUES (3);
      INSERT INTO otherSchema.fstTable (col1) VALUES (4);
      """);

    tests.add("""
      CREATE TABLE public.fstTable (col1 integer);
      INSERT INTO public.fstTable (col1) VALUES (1);
      INSERT INTO public.fstTable (col1) VALUES (2);

      CREATE TABLE public.fsttable (col1 integer);
      INSERT INTO public.fsttable (col1) VALUES (10);
      INSERT INTO public.fsttable (col1) VALUES (20);
      """);
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

  @Test(description = "[siard-1] Tests PostgreSQL files", dataProvider = "testWorkflowQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard1"})
  public void testFilesSiard1(String... file)
    throws IOException, InterruptedException, URISyntaxException, SQLException {
    assert rt_siard1.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }

  @Test(description = "[siard-2] Tests PostgreSQL files", dataProvider = "testWorkflowQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard2"})
  public void testFilesSiard2(String... file)
    throws IOException, InterruptedException, URISyntaxException, SQLException {
    assert rt_siard2.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }

  @Test(description = "[siard-2-ex] Tests PostgreSQL files", dataProvider = "testWorkflowQueriesProvider", dependsOnMethods = {
    "setup"}, groups = {"postgresql-siard2"})
  public void testFilesSiard2ex(String... file)
    throws IOException, InterruptedException, URISyntaxException, SQLException {
    assert rt_siard2ex.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
  }
}
