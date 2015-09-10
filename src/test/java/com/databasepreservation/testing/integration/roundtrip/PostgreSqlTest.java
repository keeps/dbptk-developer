package com.databasepreservation.testing.integration.roundtrip;

import com.databasepreservation.testing.integration.roundtrip.differences.PostgreSqlDumpDiffExpectations;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

@Test(groups = {"postgresql-siard1"}) public class PostgreSqlTest {
        private final String db_source = "dpttest";
        private final String db_target = "dpttest_siard";
        private final String db_tmp_username = "dpttest";
        private final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
        private File tmpFile;
        private Roundtrip rt;

        @BeforeClass public void setup() throws IOException, InterruptedException, URISyntaxException {
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

                //Files.setAttribute(p, attribute, value, options)

                rt = new Roundtrip(String.format("%s \"%s\" \"%s\" \"%s\" \"%s\"",
                  getClass().getResource("/postgreSql/scripts/setup.sh").getPath(), db_source, db_target,
                  db_tmp_username, db_tmp_password), String.format("%s \"%s\" \"%s\" \"%s\"",
                  getClass().getResource("/postgreSql/scripts/teardown.sh").getPath(), db_source, db_target,
                  db_tmp_username), "psql -q",
                  "pg_dump --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",
                  "pg_dump --format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces",

                  new String[] {"--import=PostgreSQLJDBC", "--ihostname=localhost", "--idatabase", db_source,
                    "--iusername", db_tmp_username, "--ipassword", db_tmp_password, "--ido-not-encrypt",
                    "--export=SIARD1", "--efile", Roundtrip.TMP_FILE_SIARD_VAR},

                  new String[] {"--import=SIARD1", "--ifile", Roundtrip.TMP_FILE_SIARD_VAR, "--export=PostgreSQLJDBC",
                    "--ehostname=localhost", "--edatabase", db_target, "--eusername", db_tmp_username, "--epassword",
                    db_tmp_password, "--edo-not-encrypt"}, new PostgreSqlDumpDiffExpectations(), env_var_source,
                  env_var_target);
        }

        @Test(description = "PostgreSql server is available and accessible") public void testConnection()
          throws IOException, InterruptedException {
                rt.checkConnection();
        }

        @DataProvider public Iterator<Object[]> testQueriesProvider() {
                String singleTypeAndValue = "CREATE TABLE datatypes (col1 %s);\nINSERT INTO datatypes(col1) VALUES(%s);";
                ArrayList<Object[]> tests = new ArrayList<Object[]>();

                tests.add(new String[] {singleTypeAndValue, "\"char\" NOT NULL", "'a'"});
                tests.add(new String[] {singleTypeAndValue, "bigint", "123"});
                tests.add(new String[] {singleTypeAndValue, "boolean", "TRUE"});
                tests.add(
                  new String[] {singleTypeAndValue, "bytea", "(decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'))"});
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
                //		tests.add(new String[]{singleTypeAndValue, "time without time zone", "'23:59:59.999'"});
                //		tests.add(new String[]{singleTypeAndValue, "timestamp with time zone", "'2015-01-01 23:59:59.999+8'"});
                //		tests.add(new String[]{singleTypeAndValue, "timestamp without time zone", "'2015-01-01 23:59:59.999'"});

                return tests.iterator();
        }

        @Test(description = "Tests small examples", dataProvider = "testQueriesProvider", dependsOnMethods = {
          "testConnection"}) public void testQueries(String... args) throws IOException, InterruptedException {

                String[] fields = new String[args.length - 1];
                System.arraycopy(args, 1, fields, 0, args.length - 1);

                assert rt.testTypeAndValue(args[0], fields) :
                  "Query failed: " + String.format(args[0], (Object[]) fields);
        }

        @DataProvider public Iterator<Object[]> testFilesProvider() throws URISyntaxException {
                ArrayList<Object[]> tests = new ArrayList<Object[]>();

                //tests.add(new Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes.sql").toURI())});
                //tests.add(new Path[]{Paths.get(getClass().getResource("/postgreSql/testfiles/datatypes_with_arrays.sql").toURI())});

                return tests.iterator();
        }

        @Test(description = "Tests PostgreSQL files", dataProvider = "testFilesProvider", dependsOnMethods = {
          "testConnection"}) public void testFiles(Path... file)
          throws IOException, InterruptedException, URISyntaxException {
                assert rt.testFile(file[0]) : "Roundtrip failed for file: " + file[0].toString();
        }
}
