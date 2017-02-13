package com.databasepreservation.modules;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLUtils.class);

  public static void closeQuietly(ResultSet resultSet) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        LOGGER.debug("Problem trying to close ResultSet", e);
      }
    }
  }

  public static void closeQuietly(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.debug("Problem trying to close Connection", e);
      }
    }
  }

  public static void closeQuietly(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOGGER.debug("Problem trying to close Statement", e);
      }
    }
  }
}
