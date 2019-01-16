/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CloseableUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloseableUtils.class);

  public static void closeQuietly(ResultSet resultSet) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        LOGGER.debug("Problem trying to close ResultSet", e);
      }
    }
  }

  public static void closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
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
