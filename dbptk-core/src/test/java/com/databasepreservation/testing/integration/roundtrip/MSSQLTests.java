package com.databasepreservation.testing.integration.roundtrip;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@Test(groups = {"database-roundtrip"})
public class MSSQLTests {
  // TODO: Implement more tests for MSSQL

  // Test containers for source and target databases
  private final MSSQLServerContainer<?> mssqlServerSource = new MSSQLServerContainer<>(
    "mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04").acceptLicense().withExposedPorts(1433)
    .withPassword("Your_password123");

  private final MSSQLServerContainer<?> mssqlServerTarget = new MSSQLServerContainer<>(
    "mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04").acceptLicense().withExposedPorts(1433)
    .withPassword("Your_password123");

  @BeforeClass
  void startContainer() {
    mssqlServerSource.start();
    mssqlServerTarget.start();
  }

  @AfterClass
  void stopContainer() {
    mssqlServerSource.stop();
    mssqlServerTarget.stop();
  }

  @Test
  public void testConnections() throws Exception {
    // Test source database connection
    try (
      Connection conn = DriverManager.getConnection(mssqlServerSource.getJdbcUrl(), mssqlServerSource.getUsername(),
        mssqlServerSource.getPassword());
      Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(2), "Main DB connection should be valid");
      ResultSet rs = stmt.executeQuery("SELECT DB_NAME()");
      rs.next();
      assertEquals(rs.getString(1), "master", "Database name should be 'master'");
    }

    try (
      Connection conn = DriverManager.getConnection(mssqlServerTarget.getJdbcUrl(), mssqlServerTarget.getUsername(),
        mssqlServerTarget.getPassword());
      Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(2), "Main DB connection should be valid");
      ResultSet rs = stmt.executeQuery("SELECT DB_NAME()");
      rs.next();
      assertEquals(rs.getString(1), "master", "Database name should be 'master'");
    }
  }
}
