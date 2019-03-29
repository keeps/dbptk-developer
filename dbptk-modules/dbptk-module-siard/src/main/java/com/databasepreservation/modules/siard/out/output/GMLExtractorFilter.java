package com.databasepreservation.modules.siard.out.output;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.databasepreservation.model.Reporter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.filters.IdentityFilter;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.TableStructure;

public class GMLExtractorFilter extends IdentityFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(GMLExtractorFilter.class);

  private boolean hasGeometry;
  private DatabaseStructure databaseStructure;
  private TableStructure table;
  private String tableName;
  private BufferedWriter writer;
  private Path outputDirectory;

  private DatabaseExportModule exportModule;

  public GMLExtractorFilter(Path outputDirectory) {
    super();
    this.outputDirectory = outputDirectory;
  }

  @Override
  public DatabaseExportModule migrateDatabaseTo(DatabaseExportModule exportModule) throws ModuleException {
    this.exportModule = exportModule;
    return super.migrateDatabaseTo(exportModule);
  }

  @Override
  public void initDatabase() throws ModuleException {
    try {
      outputDirectory = Files.createDirectories(outputDirectory);
    } catch (IOException e) {
      throw new ModuleException().withCause(e).withMessage("Could not create GML output directory");
    }
    super.initDatabase();
  }

  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException {
    this.databaseStructure = structure;
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    table = databaseStructure.getTableById(tableId);
    hasGeometry = false;
    for (ColumnStructure column : table.getColumns()) {
      if (column.getType().getOriginalTypeName().equalsIgnoreCase("SDO_GEOMETRY")) {
        hasGeometry = true;
        tableName = table.getName().toLowerCase();
        break;
      }
    }
    if (hasGeometry) {
      try {
        Path gmlFile = outputDirectory.resolve(tableName + ".gml");
        writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(gmlFile)));
        try {
          writer.write("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n");
          writer.write(String.format(
            "<dbptk:FeatureCollection xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://ogr.maptools.org/ %s.xsd\" \n",
            tableName));
          writer.write("  xmlns:dbptk=\"http://ogr.maptools.org/\" \n");
          writer.write("  xmlns:gml=\"http://www.opengis.net/gml\">");
        } catch (IOException e) {
          throw new ModuleException().withCause(e).withMessage("Error while writing to .gml file");
        }
      } catch (IOException e) {
        throw new ModuleException().withCause(e).withMessage("Could not create .gml file");
      }
    }

    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {

    if (hasGeometry) {
      try {
        writer.write("<gml:featureMember>\n");
        writer.write(String.format("<dbptk:multiple fid=\"%s.%d\">", tableName, row.getIndex()));
        try {

          int nCells = row.getCells().size();
          for (int i = 0; i < nCells; i++) {
            Cell cell = row.getCells().get(i);

            if (table.getColumns().get(i).getType().getOriginalTypeName().equalsIgnoreCase("SDO_GEOMETRY")) {
              writer.write("<dbptk:geometryProperty>");
              if (cell instanceof SimpleCell) {
                writer.write(((SimpleCell) cell).getSimpleData());
              } else if (cell instanceof BinaryCell) {
                try (InputStream inputStream = ((BinaryCell) cell).createInputStream()) {
                  IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8);
                } catch (IOException e) {
                  LOGGER.debug("Could not read lob data", e);
                }
              }
              writer.write("</dbptk:geometryProperty>");
            } else {
              writer.write(String.format("<dbptk:%s>", table.getColumns().get(i).getName()));

              if (cell instanceof SimpleCell) {
                writer.write(((SimpleCell) cell).getSimpleData());
              }
              writer.write(String.format("</dbptk:%s>", table.getColumns().get(i).getName()));
            }
          }
        } catch (IOException e) {
          throw new ModuleException().withCause(e).withMessage("Error while writing to .gml file");
        }
        writer.write("</dbptk:multiple>");
        writer.write("</gml:featureMember>");
      } catch (IOException e) {
        throw new ModuleException().withCause(e).withMessage("Error while writing to .gml file");
      }
    }
    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {

    if (hasGeometry) {
      try {
        writer.write("</dbptk:FeatureCollection>");
        writer.close();
      } catch (IOException e) {
        throw new ModuleException().withCause(e).withMessage("Error while writing to .gml file");
      }
    }
    this.exportModule.handleDataCloseTable(tableId);
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    super.setOnceReporter(reporter);
    this.exportModule.setOnceReporter(reporter);
  }
}
