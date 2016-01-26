
package com.databasepreservation.modules.siard.in.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.TypeConverterFactory;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

import dk.sa.xmlns.diark._1_0.tableindex.ColumnType;
import dk.sa.xmlns.diark._1_0.tableindex.ColumnsType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeysType;
import dk.sa.xmlns.diark._1_0.tableindex.PrimaryKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ReferenceType;
import dk.sa.xmlns.diark._1_0.tableindex.SiardDiark;
import dk.sa.xmlns.diark._1_0.tableindex.TableType;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKMetadataImportStrategy implements MetadataImportStrategy {

  protected final CustomLogger logger = CustomLogger.getLogger(SIARDDKMetadataImportStrategy.class);

  protected final MetadataPathStrategy metadataPathStrategy;
  protected final ContentPathImportStrategy contentPathStrategy;
  protected DatabaseStructure databaseStructure;
  protected final String importAsSchameName;

  public SIARDDKMetadataImportStrategy(MetadataPathStrategy metadataPathStrategy,
    ContentPathImportStrategy contentPathImportStrategy, String importAsSchameName) {
    this.metadataPathStrategy = metadataPathStrategy;
    this.contentPathStrategy = contentPathImportStrategy;
    this.importAsSchameName = importAsSchameName;
  }

  @Override
  public void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container) throws ModuleException {

    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardDiark.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException("Error loading JAXBContext", e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdStream = readStrategy.createInputStream(container,
      metadataPathStrategy.getXsdFilePath(SIARDDKConstants.TABLE_INDEX));
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
    } catch (SAXException e) {
      throw new ModuleException(
        "Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(SIARDDKConstants.TABLE_INDEX), e);
    }
    InputStream reader = null;
    SiardDiark xmlRoot;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      // unmarshaller.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
      // unmarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
      unmarshaller.setSchema(xsdSchema);
      // TODO: Validate file md5sum
      reader = readStrategy.createInputStream(container,
        metadataPathStrategy.getXmlFilePath(SIARDDKConstants.TABLE_INDEX));
      xmlRoot = (SiardDiark) unmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      throw new ModuleException("Error while Unmarshalling JAXB", e);
    } finally {
      try {
        xsdStream.close();
        if (reader != null) {
          reader.close();
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

  protected DatabaseStructure getDatabaseStructure(SiardDiark siardArchive) throws ModuleException {
    DatabaseStructure databaseStructure = new DatabaseStructure();
    // TODO:
    /*
     * databaseStructure.setDescription(siardArchive.getDescription());
     * databaseStructure.setArchiver(siardArchive.getArchiver());
     * databaseStructure.setArchiverContact(siardArchive.getArchiverContact());
     * databaseStructure.setDataOwner(siardArchive.getDataOwner());
     * databaseStructure.setDataOriginTimespan(siardArchive.
     * getDataOriginTimespan());
     * databaseStructure.setProducerApplication(siardArchive.
     * getProducerApplication());
     * databaseStructure.setArchivalDate(JodaUtils.xs_date_parse(siardArchive.
     * getArchivalDate()));
     * databaseStructure.setClientMachine(siardArchive.getClientMachine());
     * databaseStructure.setDatabaseUser(siardArchive.getDatabaseUser());
     */
    databaseStructure.setName(siardArchive.getDbName());
    databaseStructure.setProductName(siardArchive.getDatabaseProduct());
    databaseStructure.setSchemas(getSchemas(siardArchive));

    return databaseStructure;

  }

  protected List<SchemaStructure> getSchemas(SiardDiark siardArchive) throws ModuleException {
    SchemaStructure schemaImportAs = new SchemaStructure();
    schemaImportAs.setName(getImportAsSchameName());
    schemaImportAs.setTables(getTables(siardArchive));
    List<SchemaStructure> list = new LinkedList<SchemaStructure>();
    list.add(schemaImportAs);
    // TODO: Views
    return list;

  }

  protected List<TableStructure> getTables(SiardDiark siardArchive) throws ModuleException {
    List<TableStructure> lstTblsDptkl = new LinkedList<TableStructure>();

    if (siardArchive.getTables() != null && siardArchive.getTables().getTable() != null) {
      for (TableType tblXml : siardArchive.getTables().getTable()) {
        TableStructure tblDptkl = new TableStructure();
        tblDptkl.setSchema(getImportAsSchameName());
        tblDptkl.setName(tblXml.getName());
        tblDptkl.setId(String.format("%s.%s", tblDptkl.getSchema(), tblDptkl.getName()));
        tblDptkl.setDescription(tblXml.getDescription());
        tblDptkl.setPrimaryKey(getPrimaryKey(tblXml.getPrimaryKey()));
        tblDptkl.setForeignKeys(getForeignKeys(tblXml.getForeignKeys(), tblDptkl.getId()));
        tblDptkl.setRows(getNumberOfTblRows(tblXml.getRows(), tblXml.getName()));
        tblDptkl.setColumns(getTblColumns(tblXml.getColumns(), tblDptkl.getId()));
        contentPathStrategy.associateTableWithFolder(tblDptkl.getId(), tblXml.getFolder());
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
        columnDptkl.setType(
          TypeConverterFactory.getSQL99TypeConverter().getType(columnXml.getType(), columnXml.getTypeOriginal()));
        // TODO: Consider if columnXml.getFunctionalDescription() should be
        // merged into this as well.
        columnDptkl.setDescription(columnXml.getDescription());
        columnDptkl.setDefaultValue(columnXml.getDefaultValue());
        columnDptkl.setNillable(columnXml.isNullable());

        // TODO LOB
        // contentPathStrategy.associateColumnWithFolder(columnDptkl.getId(),);
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
      return numRows.longValueExact();
    } catch (ArithmeticException e) {
      throw new ModuleException(
        "Unable to import table [" + tableName + "], as the number of rows [" + numRows
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