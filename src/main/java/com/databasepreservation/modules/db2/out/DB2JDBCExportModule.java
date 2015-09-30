package com.databasepreservation.modules.db2.out;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.db2.DB2Helper;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import org.apache.log4j.Logger;
import org.w3c.util.InvalidDateException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/**
 * @author Miguel Coutada
 */
public class DB2JDBCExportModule extends JDBCExportModule {

  private final Logger logger = Logger.getLogger(DB2JDBCExportModule.class);

  /**
   * Db2 JDBC export module constructor
   *
   * @param hostname
   *          the host name of Db2 Server
   * @param port
   *          the port that the Db2 server is listening
   * @param database
   *          the name of the database to import from
   * @param username
   *          the name of the user to use in connection
   * @param password
   *          the password of the user to use in connection
   */
  public DB2JDBCExportModule(String hostname, int port, String database, String username, String password) {
    super("com.ibm.db2.jcc.DB2Driver", "jdbc:db2://" + hostname + ":" + port + "/" + database + ":user=" + username
      + ";password=" + password + ";", new DB2Helper());

  }

  public void finishDatabase() throws ModuleException {
    if (databaseStructure != null) {
      logger.info("Handling foreign keys is not yet supported!");
      // handleForeignKeys();
      commit();
    }
  }

  protected void handleSimpleTypeDateTimeDataCell(String data, PreparedStatement ps, int index, Cell cell, Type type)
    throws InvalidDateException, SQLException {
    SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
    if (dateTime.getTimeDefined()) {
      if (type.getSql99TypeName().equalsIgnoreCase("TIMESTAMP")) {
        if (data != null) {
          logger.debug("timestamp before: " + data);
          Calendar cal = javax.xml.bind.DatatypeConverter.parseDateTime(data);
          Timestamp sqlTimestamp = new Timestamp(cal.getTimeInMillis());
          logger.debug("timestamp after: " + sqlTimestamp.toString());
          ps.setTimestamp(index, sqlTimestamp);
        } else {
          ps.setNull(index, Types.TIMESTAMP);
        }
      } else {
        if (data != null) {
          logger.debug("TIME before: " + data);
          Time sqlTime = Time.valueOf(data);
          logger.debug("TIME after: " + sqlTime.toString());
          ps.setTime(index, sqlTime);
        } else {
          ps.setNull(index, Types.TIME);
        }
      }
    } else {
      if (data != null) {
        logger.debug("DATE before: " + data);
        java.sql.Date sqlDate = java.sql.Date.valueOf(data);
        logger.debug("DATE after: " + sqlDate.toString());
        ps.setDate(index, sqlDate);
      } else {
        ps.setNull(index, Types.DATE);
      }
    }
  }
}
