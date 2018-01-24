/**
 *
 */
package com.databasepreservation.modules.sqlFile.out;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.modules.SQLHelper;
import com.databasepreservation.modules.SQLHelper.CellSQLHandler;

/**
 * @author Luis Faria
 */
public class SQLFileExportModule implements DatabaseExportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLFileExportModule.class);

  protected File sqlFile;

  protected FileOutputStream sqlOutput;

  protected BufferedWriter sqlWriter;

  protected SQLHelper sqlHelper;

  protected DatabaseStructure structure;

  protected TableStructure currentTable;

  protected Reporter reporter;

  /**
   * Create a new SQLFile export module, specifying the SQL helper to use
   *
   * @param sqlFile
   *          the file where to dump the SQL
   * @param sqlHelper
   *          the SQL helper
   * @throws ModuleException
   */
  public SQLFileExportModule(File sqlFile, SQLHelper sqlHelper) throws ModuleException {
    this.sqlFile = sqlFile;
    this.sqlHelper = sqlHelper;
    try {
      sqlOutput = new FileOutputStream(sqlFile);
      sqlWriter = new BufferedWriter(new OutputStreamWriter(sqlOutput));
    } catch (FileNotFoundException e) {
      throw new ModuleException("Error creating output writer", e);
    }
    structure = null;
    currentTable = null;
  }

  /**
   * Encode a string with escapes:
   * <ul>
   * <li>'\\': '\\\\'</li>
   * </ul>
   *
   * @param string
   *          the original string
   * @return the escaped string
   */
  public static String escapeString(String string) {
    String ret = string;
    ret = ret.replaceAll("\\\\", "\\\\\\\\");
    ret = ret.replaceAll("'", "''");
    return ret.equals(string) ? "'" + ret + "'" : "E'" + ret + "'";
  }

  /**
   * Escape string literal
   *
   * @param in
   *          the original string input stream
   * @param out
   *          the escaped string output stream
   * @throws IOException
   */
  public static void escapeStringLiteral(InputStream in, OutputStream out) throws IOException {
    // BufferedInputStream buffin = new BufferedInputStream(in);
    // BufferedOutputStream buffout = new BufferedOutputStream(out);

    out.write("E'".getBytes());

    int ibyte = in.read();
    while (ibyte != -1) {
      switch (ibyte) {
        case '\'':
          out.write("''".getBytes());
          break;
        case '\\':
          out.write("\\\\".getBytes());
          break;
        default:
          if (ibyte > 0 && ibyte < 31 || ibyte > 127 && ibyte <= 255) {
            out.write(("\\" + ibyte).getBytes());
          } else {
            out.write(ibyte);
          }
          break;
      }

      ibyte = in.read();
    }

    out.write("'".getBytes());
  }

  /**
   * Encode binary with escapes:
   * <ul>
   * <li>'\000': "\\000"</li>
   * <li>'\'': '\\\''</li>
   * <li>'\\': '\\\\'</li>
   * <li>0-31 and 127-255: \xxx</li>
   * </ul>
   *
   * @param in
   *          the binary input stream where to read the original binary
   * @param out
   *          the binary output stream where to write the encoded binary
   * @throws IOException
   */
  public static void escapeBinary(InputStream in, OutputStream out) throws IOException {

    BufferedInputStream bin = new BufferedInputStream(in);
    BufferedOutputStream bout = new BufferedOutputStream(out);

    bout.write("E'".getBytes());
    int ibyte = bin.read();
    while (ibyte != -1) {

      switch (ibyte) {
        case 0:
          bout.write("\\\\000".getBytes());
          break;
        case '\'':
          bout.write("''''".getBytes());
          break;
        case '\\':
          bout.write("\\\\\\\\".getBytes());
          break;
        default:
          if (ibyte > 0 && ibyte < 31 || ibyte > 127 && ibyte <= 255) {
            bout.write(("\\\\" + ibyte).getBytes());
          } else {
            bout.write(ibyte);
          }
          break;
      }

      ibyte = bin.read();
    }
    bout.write("'::bytea".getBytes());
    bout.flush();

  }

  /**
   * Escape binary and then escape string literal
   *
   * @param in
   *          the binary input stream
   * @param out
   *          the doubble escaped binary output stream
   * @throws IOException
   * @deprecated use escape binary instead
   */
  @Deprecated
  public static void escapeBinaryAsStringLiteral(final InputStream in, final OutputStream out) throws IOException {
    final PipedInputStream bin = new PipedInputStream();
    final PipedOutputStream bout = new PipedOutputStream(bin);

    Runnable writerRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          escapeBinary(in, bout);
          bout.close();
        } catch (IOException e) {
          LOGGER.error("Error escaping binary into circular buffer", e);
        }
      }
    };
    new Thread(writerRunnable).start();
    escapeStringLiteral(bin, out);
  }

  /**
   * Gets custom settings set by the export module that modify behaviour of the
   * import module.
   *
   * @throws ModuleException
   */
  @Override
  public ModuleSettings getModuleSettings() throws ModuleException {
    return new ModuleSettings();
  }

  @Override
  public void initDatabase() throws ModuleException {
    // nothing to do
  }

  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    // nothing to do
  }

  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException {
    try {
      this.structure = structure;
      for (SchemaStructure schema : structure.getSchemas()) {
        for (TableStructure table : schema.getTables()) {
          sqlWriter.write(sqlHelper.createTableSQL(table) + ";\n");
          String pkeySQL = sqlHelper.createPrimaryKeySQL(table.getId(), table.getPrimaryKey());
          LOGGER.debug("PKEY: " + sqlHelper.createPrimaryKeySQL(table.getId(), table.getPrimaryKey()));
          if (pkeySQL != null) {
            sqlWriter.write(pkeySQL + ";\n");
          }
        }
      }
      sqlWriter.flush();
    } catch (IOException e) {
      throw new ModuleException("Error while handling structure", e);
    }
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    // do nothing
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    if (structure != null) {
      currentTable = structure.getTableById(tableId);
    } else {
      throw new ModuleException("Table " + tableId + " opened before struture was defined");
    }
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    if (currentTable != null) {
      byte[] rowSQL = sqlHelper.createRowSQL(currentTable, row, new CellSQLHandler() {

        @Override
        public byte[] createCellSQL(Cell cell, ColumnStructure column) throws InvalidDataException, ModuleException {
          byte[] ret;
          if (cell instanceof SimpleCell) {
            SimpleCell simple = (SimpleCell) cell;
            if (simple.getSimpleData() == null) {
              ret = "NULL".getBytes();
            } else if (column.getType() instanceof SimpleTypeNumericExact
              || column.getType() instanceof SimpleTypeNumericApproximate) {
              try {
                ret = simple.getSimpleData().getBytes("UTF-8");
              } catch (UnsupportedEncodingException e) {
                throw new ModuleException("Unsupported encoding", e);
              }
            } else {
              ret = (escapeString(simple.getSimpleData())).getBytes();
            }

          } else if (cell instanceof BinaryCell) {
            BinaryCell bin = (BinaryCell) cell;
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            InputStream inputStream = bin.createInputStream();
            try {
              escapeBinary(inputStream, bout);
            } catch (IOException e) {
              throw new ModuleException("Error getting binary from binary cell", e);
            }

            ret = bout.toByteArray();

            // clean resources
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(bout);
            bin.cleanResources();
          } else if (cell instanceof ComposedCell) {
            throw new ModuleException("Composed cell export not yet supported");
          } else {
            throw new ModuleException(cell.getClass().getSimpleName() + " not supported");
          }
          return ret;
        }

      });
      byte[] rowSQLCollon = new byte[rowSQL.length + 2];
      System.arraycopy(rowSQL, 0, rowSQLCollon, 0, rowSQL.length);
      System.arraycopy(";\n".getBytes(), 0, rowSQLCollon, rowSQL.length, 2);
      try {
        sqlOutput.write(rowSQLCollon);
      } catch (IOException e) {
        throw new ModuleException("Error writing row to file", e);
      }
    }
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    currentTable = null;
  }

  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    // do nothing
  }

  @Override
  public void finishDatabase() throws ModuleException {
    for (SchemaStructure schema : structure.getSchemas()) {
      for (TableStructure table : schema.getTables()) {
        for (ForeignKey fkey : table.getForeignKeys()) {
          try {
            String fkeySQL = sqlHelper.createForeignKeySQL(table, fkey);
            sqlWriter.write(fkeySQL + ";\n");
            sqlWriter.flush();
          } catch (IOException e) {
            throw new ModuleException("Error writing foreign key: " + fkey, e);
          }
        }
      }
    }
  }

  /**
   * Provide a reporter through which potential conversion problems should be
   * reported. This reporter should be provided only once for the export module
   * instance.
   *
   * @param reporter
   *          The initialized reporter instance.
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }
}
