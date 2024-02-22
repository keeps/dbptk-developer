/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.mysql;

import java.sql.SQLException;

import com.databasepreservation.model.exception.ConnectionException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.PermissionDeniedException;
import com.databasepreservation.model.exception.ServerException;
import com.databasepreservation.model.modules.ExceptionNormalizer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySQLExceptionNormalizer implements ExceptionNormalizer {
  private static final MySQLExceptionNormalizer instance = new MySQLExceptionNormalizer();

  /**
   * Although this class is not necessarily a singleton, it can be used like a
   * singleton to avoid creating multiple (similar) instances.
   *
   * @return an ExceptionNormalizer
   */
  public static ExceptionNormalizer getInstance() {
    return instance;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    if (exception instanceof SQLException) {
      SQLException e = (SQLException) exception;

      // source: https://dev.mysql.com/doc/refman/5.5/en/server-error-reference.html
      switch (e.getErrorCode()) {
        case 1044: // Access denied for user '%s'@'%s' to database '%s'
        case 1045: // Access denied for user '%s'@'%s' (using password: %s)
        case 1130: // Host '%s' is not allowed to connect to this MySQL server
        case 1142: // %s command denied to user '%s'@'%s' for table '%s'
        case 1143: // %s command denied to user '%s'@'%s' for column '%s' in table '%s'
        case 1211: // '%s'@'%s' is not allowed to create new users
        case 1227: // Access denied; you need (at least one of) the %s privilege(s) for this
          // operation
        case 1370: // %s command denied to user '%s'@'%s' for routine '%s'
        case 1410: // You are not allowed to create a user with GRANT
        case 1644: // Access denied for user '%s'@'%s' to database '%s'
        case 1645: // Access denied for user '%s'@'%s' (using password: %s)
        case 1698: // Access denied for user '%s'@'%s'
          return new PermissionDeniedException().withCause(e);

        case 1040: // Too many connections
        case 1042: // Can't get hostname for your address
        case 1043: // Bad handshake
        case 1046: // No database selected
        case 1081: // Can't create IP socket
        case 1152: // Aborted connection %ld to db: '%s' user: '%s' (%s)
        case 1153: // Got a packet bigger than 'max_allowed_packet' bytes
        case 1154: // Got a read error from the connection pipe
        case 1155: // Got an error from fcntl()
        case 1156: // Got packets out of order
        case 1157: // Couldn't uncompress communication packet
        case 1158: // Got an error reading communication packets
        case 1159: // Got timeout reading communication packets
        case 1160: // Got an error writing communication packets
        case 1161: // Got timeout writing communication packets
        case 1162: // Result string is longer than 'max_allowed_packet' bytes
        case 1184: // Aborted connection %ld to db: '%s' user: '%s' host: '%s' (%s)
        case 1301: // Result of %s() was larger than max_allowed_packet (%ld) - truncated
          return new ConnectionException().withCause(e);

        case 1021: // Disk full (%s); waiting for someone to free some space...
        case 1030: // Got error %d from storage engine
        case 1037: // Out of memory; restart server and try again (needed %d bytes)
        case 1038: // Out of sort memory, consider increasing server sort buffer size
        case 1041: // Out of memory; check if mysqld or some other process uses all available
                   // memory; if not, you may have to use 'ulimit' to allow mysqld to use more
                   // memory or you can add more swap space
        case 1053: // Server shutdown in progress
        case 1080: // Forcing close of thread %ld user: '%s'
        case 1148: // The used command is not allowed with this MySQL version
        case 1189: // Net error reading from master
        case 1190: // Net error writing to master
        case 1218: // Error connecting to master: %s
          return new ServerException().withCause(e);
      }
    }

    // these are also SQLExceptions, but prefer using the codes above since they are
    // more specific and then check if it is a "connection problem"

    if (exception instanceof com.mysql.cj.exceptions.CJCommunicationsException) {
      return new ConnectionException().withCause(exception);
    }

    return null; // handle this null on the caller side
  }
}
