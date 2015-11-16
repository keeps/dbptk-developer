package com.databasepreservation.testing.integration.roundtrip;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import com.databasepreservation.Main;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * Attempt at testing UDT types in SQL Server
 */
@Test(groups = {"sqlserver-siard1"})
public class SqlServerTest {
  private final String db_source = "dbptk_test_source";
  private final String db_target = "dbptk_test_target";
  private final String db_tmp_username = "dpttest";
  private final String db_tmp_password = "#123aBc" + RandomStringUtils.randomAlphabetic(5);

  // JDBC driver name and database URL
  private final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  private final String SERVER_PROTOCOL = "jdbc:sqlserver://";
  private final String SERVER_NAME; // from environment variable
  private final String INSTANCE_NAME; // from environment variable
  private final String USERNAME; // from environment variable
  private final String PASSWORD; // from environment variable
  private final String INTEGRATED_SECURITY = "false;";
  private final String ENCRYPT = "false";
  private final String SIARD_FILE;

  public SqlServerTest() throws IOException {
    Map<String, String> env = System.getenv();
    SERVER_NAME = env.get("DPT_MSSQL_SERVER");
    INSTANCE_NAME = env.get("DPT_MSSQL_INSTANCE");
    USERNAME = env.get("DPT_MSSQL_USER");
    PASSWORD = env.get("DPT_MSSQL_PASS");
    SIARD_FILE = File.createTempFile("sqlserver", ".siard").toString();
  }

  private Connection setup() throws SQLException {
    StringBuilder dbUrl = new StringBuilder();
    dbUrl.append(SERVER_PROTOCOL).append(SERVER_NAME).append("\\").append(INSTANCE_NAME);

    dbUrl.append(";user=").append(USERNAME);
    dbUrl.append(";password=").append(PASSWORD);
    dbUrl.append(";integratedSecurity=").append(INTEGRATED_SECURITY);
    dbUrl.append(";encrypt=").append(ENCRYPT);

    Connection c = DriverManager.getConnection(dbUrl.toString());
    Statement s = ((SQLServerConnection) c).createStatement();

    for (String dbname : Arrays.asList(db_source)) {
      StringBuilder exe = new StringBuilder();

      // stop access to database and drop it
      exe.append("WHILE EXISTS(select NULL from sys.databases where name='").append(dbname).append("')\n");
      exe.append("BEGIN\n");
      exe.append("    DECLARE @SQL varchar(max)\n");
      exe.append("    SELECT @SQL = COALESCE(@SQL,'') + 'Kill ' + Convert(varchar, SPId) + ';'\n");
      exe.append("    FROM master..sysprocesses\n");
      exe.append("    WHERE DBId = DB_ID(N'").append(dbname).append("') AND SPId <> @@SPId\n");
      exe.append("    EXEC(@SQL)\n");
      exe.append("    DROP DATABASE [").append(dbname).append("]\n");
      exe.append("END\n");
      s.execute(exe.toString());

      // create the database
      s.execute("CREATE DATABASE " + dbname);
    }

    s.close();
    c.close();

    // -----------------------------------

    dbUrl = new StringBuilder();
    dbUrl.append(SERVER_PROTOCOL).append(SERVER_NAME).append("\\").append(INSTANCE_NAME);

    dbUrl.append(";database=").append(db_source);
    dbUrl.append(";user=").append(USERNAME);
    dbUrl.append(";password=").append(PASSWORD);
    dbUrl.append(";integratedSecurity=").append(INTEGRATED_SECURITY);
    dbUrl.append(";encrypt=").append(ENCRYPT);

    c = DriverManager.getConnection(dbUrl.toString());
    s = ((SQLServerConnection) c).createStatement();

    // Create the UDT:
    s.execute("EXEC sp_addtype UDT_phone, 'VARCHAR(12)', 'NOT NULL'");

    // This code will create the default:
    s.execute("CREATE DEFAULT def_phone AS 'Unknown'");

    // This code will bind it to the UDT:
    s.execute("EXEC sp_bindefault 'def_phone', 'UDT_phone'");

    // This is the code for the rule:
    s.execute("CREATE RULE rule_phone AS (@phone='UNKNOWN') OR (LEN(@phone)=12 AND ISNUMERIC(LEFT(@phone,3))=1 AND SUBSTRING(@phone,4,1)=' ' AND ISNUMERIC(SUBSTRING(@phone,5,3))=1 AND SUBSTRING(@phone,8,1)='-' AND ISNUMERIC(RIGHT(@phone,4))=1 )");

    // Binding the rule to the UDT:
    s.execute("EXEC sp_bindrule 'rule_phone', 'UDT_phone'");

    // Creating a table containing the UDT:
    s.execute("CREATE TABLE dbo.Contacts ( phone_num UDT_phone NOT NULL, fax_num UDT_phone NULL) ON [PRIMARY]");

    // sample row
    s.execute("INSERT INTO dbo.Contacts VALUES ('123 567-9098', '123 567-1234');");

    s.close();
    c.close();
    return null;
  }

  @Test(description = "Sql server is available and accessible")
  public void testConnection() throws IOException, InterruptedException, SQLException {
    setup();

    Main.internal_main("-i", "microsoft-sql-server", "-is", SERVER_NAME, "-iin", INSTANCE_NAME, "-idb", db_source, "-iu",
      USERNAME, "-ip", PASSWORD, "-e", "siard-1", "-ef", SIARD_FILE, "-ide");

  }
}
