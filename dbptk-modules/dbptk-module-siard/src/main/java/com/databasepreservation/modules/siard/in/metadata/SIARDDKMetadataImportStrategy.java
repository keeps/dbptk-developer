package com.databasepreservation.modules.siard.in.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQL99StandardDatatypeImporter;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQLStandardDatatypeImporter;
import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

import dk.sa.xmlns.diark._1_0.tableindex.ColumnType;
import dk.sa.xmlns.diark._1_0.tableindex.ColumnsType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeysType;
import dk.sa.xmlns.diark._1_0.tableindex.PrimaryKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ReferenceType;
import dk.sa.xmlns.diark._1_0.tableindex.SiardDiark;
import dk.sa.xmlns.diark._1_0.tableindex.TableType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewType;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKMetadataImportStrategy implements MetadataImportStrategy {

  protected final Logger logger = LoggerFactory.getLogger(SIARDDKMetadataImportStrategy.class);

  protected final SIARDDKPathImportStrategy pathStrategy;
  protected DatabaseStructure databaseStructure;
  protected final String importAsSchameName;
  private int currentTableIndex = 1;

  private SQLStandardDatatypeImporter sqlStandardDatatypeImporter;
  private Reporter reporter;

  public SIARDDKMetadataImportStrategy(SIARDDKPathImportStrategy pathStrategy, String importAsSchameName) {
    this.pathStrategy = pathStrategy;
    this.importAsSchameName = importAsSchameName;
    sqlStandardDatatypeImporter = new SQL99StandardDatatypeImporter();
  }

  @Override
  public void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container, ModuleSettings moduleSettings)
    throws ModuleException {
    FolderReadStrategyMD5Sum readStrategyMD5Sum = null;
    if (!(readStrategy instanceof FolderReadStrategyMD5Sum)) {
      throw new IllegalArgumentException(
        "The current implemenation of SIARDDKMetadataImportStrategy requires relies on the FolderReadStrategyMD5Sum (should be passed to loadMetadata ).");
    }
    readStrategyMD5Sum = (FolderReadStrategyMD5Sum) readStrategy;
    pathStrategy.parseFileIndexMetadata();

    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardDiark.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException("Error loading JAXBContext", e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdInputStream = readStrategyMD5Sum.createInputStream(container,
      pathStrategy.getXsdFilePath(SIARDDKConstants.TABLE_INDEX));

    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdInputStream));
    } catch (SAXException e) {
      throw new ModuleException(
        "Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.TABLE_INDEX), e);
    }
    DigestInputStream inputStreamXml = null;
    SiardDiark xmlRoot;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(xsdSchema);
      inputStreamXml = readStrategyMD5Sum.createInputStream(container,
        pathStrategy.getXmlFilePath(SIARDDKConstants.TABLE_INDEX), pathStrategy.getTabelIndexExpectedMD5Sum());
      xmlRoot = (SiardDiark) unmarshaller.unmarshal(inputStreamXml);
    } catch (JAXBException e) {
      throw new ModuleException("Error while Unmarshalling JAXB", e);
    } finally {
      try {
        xsdInputStream.close();
        if (inputStreamXml != null) {
          readStrategyMD5Sum.closeAndVerifyMD5Sum(inputStreamXml);
        }
      } catch (IOException e) {
        logger.debug("Could not close xsdStream", e);
      }
    }

    databaseStructure = getDatabaseStructure(xmlRoot);

  }

  @Override
  public DatabaseStructure getDatabaseStructure() throws ModuleException {
    if (databaseStructure != null) {
      return databaseStructure;
    } else {
      throw new ModuleException("getDatabaseStructure must not be called before loadMetadata");
    }
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    sqlStandardDatatypeImporter.setOnceReporter(reporter);
  }

  protected DatabaseStructure getDatabaseStructure(SiardDiark siardArchive) throws ModuleException {
    DatabaseStructure databaseStructure = new DatabaseStructure();
    databaseStructure.setName(siardArchive.getDbName());
    databaseStructure.setProductName(siardArchive.getDatabaseProduct());
    databaseStructure.setSchemas(getSchemas(siardArchive));

    return databaseStructure;

  }

  protected List<SchemaStructure> getSchemas(SiardDiark siardArchive) throws ModuleException {
    SchemaStructure schemaImportAs = new SchemaStructure();
    schemaImportAs.setName(getImportAsSchameName());
    schemaImportAs.setTables(getTables(siardArchive));
    schemaImportAs.setViews(getViews(siardArchive));
    List<SchemaStructure> list = new LinkedList<SchemaStructure>();
    list.add(schemaImportAs);

    return list;

  }

  protected List<ViewStructure> getViews(SiardDiark siardArchive) {
    List<ViewStructure> lstViewsDptkl = new LinkedList<ViewStructure>();
    if (siardArchive.getViews() != null && siardArchive.getViews().getView() != null) {
      for (ViewType viewXml : siardArchive.getViews().getView()) {
        ViewStructure viewDptkl = new ViewStructure();
        if (StringUtils.isNotBlank(viewXml.getDescription())) {
          viewDptkl.setDescription(viewXml.getDescription());
        }
        viewDptkl.setName(viewXml.getName());
        viewDptkl.setQueryOriginal(viewXml.getQueryOriginal());
        // NOTICE: As siard-dk only support defining the query original
        // attribute -
        // we'll use it for both the query and the query original field in the
        // internal representation of the view.
        viewDptkl.setQuery(viewXml.getQueryOriginal());
        lstViewsDptkl.add(viewDptkl);
      }
    }
    return lstViewsDptkl;
  }

  protected List<TableStructure> getTables(SiardDiark siardArchive) throws ModuleException {
    List<TableStructure> lstTblsDptkl = new LinkedList<TableStructure>();

    if (siardArchive.getTables() != null && siardArchive.getTables().getTable() != null) {
      for (TableType tblXml : siardArchive.getTables().getTable()) {
        TableStructure tblDptkl = new TableStructure();
        tblDptkl.setIndex(currentTableIndex++);
        tblDptkl.setSchema(getImportAsSchameName());
        tblDptkl.setName(tblXml.getName());
        tblDptkl.setId(String.format("%s.%s", tblDptkl.getSchema(), tblDptkl.getName()));
        tblDptkl.setDescription(tblXml.getDescription());
        tblDptkl.setPrimaryKey(getPrimaryKey(tblXml.getPrimaryKey()));
        tblDptkl.setForeignKeys(getForeignKeys(tblXml.getForeignKeys(), tblDptkl.getId()));
        tblDptkl.setRows(getNumberOfTblRows(tblXml.getRows(), tblXml.getName()));
        tblDptkl.setColumns(getTblColumns(tblXml.getColumns(), tblDptkl.getId()));
        pathStrategy.associateTableWithFolder(tblDptkl.getId(), tblXml.getFolder());
        lstTblsDptkl.add(tblDptkl);
      }
    }
    return lstTblsDptkl;
  }

  protected List<ColumnStructure> getTblColumns(ColumnsType columnsXml, String tableId) throws ModuleException {
    List<ColumnStructure> lstColumnsDptkl = new LinkedList<ColumnStructure>();
    if (columnsXml != null && columnsXml.getColumn() != null) {
      for (ColumnType columnXml : columnsXml.getColumn()) {
        ColumnStructure columnDptkl = new ColumnStructure();
        columnDptkl.setName(columnXml.getName());
        columnDptkl.setId(String.format("%s.%s", tableId, columnDptkl.getName()));
        String typeOriginal = StringUtils.isNotBlank(columnXml.getTypeOriginal()) ? columnXml.getTypeOriginal() : null;
        columnDptkl
          .setType(sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
            "<information unavailable>", "<information unavailable>", columnXml.getType(), typeOriginal));
        columnDptkl.setDescription(columnXml.getDescription());
        String defaultValue = StringUtils.isNotBlank(columnXml.getDefaultValue()) ? columnXml.getDefaultValue() : null;
        columnDptkl.setDefaultValue(defaultValue);
        columnDptkl.setNillable(columnXml.isNullable());
        lstColumnsDptkl.add(columnDptkl);
      }
    }
    return lstColumnsDptkl;
  }

  protected Type getType(String type) {

    return null;
  }

  protected long getNumberOfTblRows(BigInteger numRows, String tableName) throws ModuleException {
    try {
      return numRows.longValue();
    } catch (ArithmeticException e) {
      throw new ModuleException("Unable to import table [" + tableName + "], as the number of rows [" + numRows
        + "] exceeds the max value of the long datatype used to store the number.(Consult the vendor/a programmer for a fix of this problem, if needed)",
        e);
    }
  }

  protected PrimaryKey getPrimaryKey(PrimaryKeyType primaryKeyXml) {
    PrimaryKey keyDptkl = new PrimaryKey();
    keyDptkl.setName(primaryKeyXml.getName());
    keyDptkl.setColumnNames(primaryKeyXml.getColumn());
    return keyDptkl;
  }

  protected List<ForeignKey> getForeignKeys(ForeignKeysType foreignKeysXml, String tableId) {
    List<ForeignKey> lstForeignKeyDptkl = new LinkedList<ForeignKey>();
    if (foreignKeysXml != null) {
      for (ForeignKeyType foreignKeyXml : foreignKeysXml.getForeignKey()) {
        ForeignKey foreignKeyDptkl = new ForeignKey();
        foreignKeyDptkl.setReferencedSchema(getImportAsSchameName());
        foreignKeyDptkl.setName(foreignKeyXml.getName());
        foreignKeyDptkl.setReferencedTable(foreignKeyXml.getReferencedTable());
        foreignKeyDptkl.setReferences(getReferences(foreignKeyXml.getReference()));
        foreignKeyDptkl.setId(String.format("%s.%s", tableId, foreignKeyDptkl.getName()));
        lstForeignKeyDptkl.add(foreignKeyDptkl);
      }
    }
    return lstForeignKeyDptkl;
  }

  protected List<Reference> getReferences(List<ReferenceType> referencesXml) {
    List<Reference> refsDptkld = new LinkedList<Reference>();
    if (referencesXml != null) {
      for (ReferenceType referenceTypeXml : referencesXml) {
        Reference refDptkld = new Reference();
        refDptkld.setColumn(referenceTypeXml.getColumn());
        refDptkld.setReferenced(referenceTypeXml.getReferenced());
        refsDptkld.add(refDptkld);
      }
    }
    return refsDptkld;
  }

  public String getImportAsSchameName() {
    return importAsSchameName;
  }

}
