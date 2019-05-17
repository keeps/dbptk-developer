/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.metadata.SIARDDatabaseMetadata;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.modules.edits.EditImportModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD21MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.update.MetadataUpdateStrategy;
import com.databasepreservation.modules.siard.out.update.UpdateStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDImportEdit implements EditImportModule {
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final MetadataImportStrategy metadataImportStrategy;
  private MetadataExportStrategy metadataExportStrategy;

  private Reporter reporter;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDImportEdit.class);

  private static final String METADATA_FILENAME = "metadata";

  public SIARDImportEdit(SIARDArchiveContainer mainContainer, ReadStrategy readStrategy,
    MetadataImportStrategy metadataImportStrategy) {
    this.readStrategy = readStrategy;
    this.mainContainer = mainContainer;
    this.metadataImportStrategy = metadataImportStrategy;
  }

  @Override
  public DatabaseStructure getMetadata() throws ModuleException {
    ModuleSettings moduleSettings = new ModuleSettings();

    LOGGER.info("Importing SIARD version {}", mainContainer.getVersion().getDisplayName());
    DatabaseStructure dbStructure;

    try {
      metadataImportStrategy.loadMetadata(readStrategy, mainContainer, moduleSettings);

      dbStructure = metadataImportStrategy.getDatabaseStructure();

    } finally {
      readStrategy.finish(mainContainer);
    }
    return dbStructure;
  }

  @Override
  public void saveMetadata(DatabaseStructure dbStructure) throws ModuleException {

    SIARD2MetadataPathStrategy siard2MetadataPathStrategy = new SIARD2MetadataPathStrategy();

    SIARD2ContentPathExportStrategy contentPathExportStrategy = new SIARD2ContentPathExportStrategy();

    SIARD21MetadataExportStrategy metadataExportStrategy = new SIARD21MetadataExportStrategy(siard2MetadataPathStrategy,
      contentPathExportStrategy, false);

    UpdateStrategy updateStrategy = new MetadataUpdateStrategy();

    metadataExportStrategy.setOnceReporter(reporter);
    metadataExportStrategy.updateMetadataXML(dbStructure, mainContainer, updateStrategy);
  }

  public List<String> getDescriptiveSIARDMetadataKeys() throws ModuleException {
    SIARD2MetadataPathStrategy siard2MetadataPathStrategy = new SIARD2MetadataPathStrategy();
    ZipReadStrategy zipReadStrategy = new ZipReadStrategy();
    zipReadStrategy.setup(mainContainer);

    List<String> descriptiveMetadata = new ArrayList<>();

    try (InputStream XSDStream = zipReadStrategy.createInputStream(mainContainer,
      siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))) {
      Document doc = getDocument(XSDStream);
      String xpathExpression = "//xs:element[@name='siardArchive']/xs:complexType/xs:sequence/xs:element/@name";

      descriptiveMetadata.addAll(getSIARDMetadata(doc, xpathExpression));

      descriptiveMetadata.remove("lobFolder");
      descriptiveMetadata.remove("schemas");
      descriptiveMetadata.remove("users");
      descriptiveMetadata.remove("roles");
      descriptiveMetadata.remove("privileges");

    } catch (ParserConfigurationException e) {
      throw new ModuleException()
        .withMessage("Error parsing the XSD file: " + siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Error open the XSD file: " + siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))
        .withCause(e);
    }

    zipReadStrategy.finish(mainContainer);

    return descriptiveMetadata;
  }

  @Override
  public List<SIARDDatabaseMetadata> getDatabaseMetadataKeys() throws ModuleException {
    SIARD2MetadataPathStrategy siard2MetadataPathStrategy = new SIARD2MetadataPathStrategy();
    ZipReadStrategy zipReadStrategy = new ZipReadStrategy();
    zipReadStrategy.setup(mainContainer);

    List<SIARDDatabaseMetadata> SIARDDatabaseMetadataKeys = new ArrayList<>();

    try (InputStream xmlStream = zipReadStrategy.createInputStream(mainContainer,
      siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))) {

      Document doc = getDocument(xmlStream);

      String xpathExpressionSchemas = "/ns:siardArchive/ns:schemas/ns:schema";
      String xpathExpressionUsers = "/ns:siardArchive/ns:users/ns:user/ns:name/text()";
      String xpathExpressionRoles = "/ns:siardArchive/ns:roles/ns:role/ns:name/text()";
      // String xpathExpressionPrivileges =
      // "/ns:siardArchive/ns:privileges/ns:privilege/ns:name/text()"; -- confirmar os
      // campos a usar para identificar unicamente um privilégio

      List<SIARDDatabaseMetadata> metadataSchema = getSchemaMetadata(doc, xpathExpressionSchemas);

      SIARDDatabaseMetadataKeys.addAll(metadataSchema);

    } catch (ParserConfigurationException e) {
      throw new ModuleException()
        .withMessage("Error parsing the XML file: " + siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Error open the XSD file: " + siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))
        .withCause(e);
    }

    zipReadStrategy.finish(mainContainer);

    return SIARDDatabaseMetadataKeys;
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    metadataImportStrategy.setOnceReporter(reporter);
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }

  private static List<SIARDDatabaseMetadata> getSchemaMetadata(Document document, String xpathExpression) {
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath.setNamespaceContext(new NamespaceContext() {
      @Override
      public Iterator getPrefixes(String arg0) {
        return null;
      }

      @Override
      public String getPrefix(String arg0) {
        return null;
      }

      @Override
      public String getNamespaceURI(String arg0) {
        if ("ns".equals(arg0)) {
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    List<SIARDDatabaseMetadata> siardatabaseMetadata = new ArrayList<>();

    try {
      XPathExpression expr = xpath.compile(xpathExpression);

      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);

        String schemaName = schema.getElementsByTagName("name").item(0).getTextContent();
        SIARDDatabaseMetadata dbMetadata = new SIARDDatabaseMetadata();
        dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
        siardatabaseMetadata.add(dbMetadata);

        NodeList tableNodes = schema.getElementsByTagName("table");
        for (int j = 0; j < tableNodes.getLength(); j++) {
          Element table = (Element) tableNodes.item(j);
          String tableName = table.getElementsByTagName("name").item(0).getTextContent();
          dbMetadata = new SIARDDatabaseMetadata();
          dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
          dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
          siardatabaseMetadata.add(dbMetadata);

          NodeList columnsNodes = table.getElementsByTagName("columns");
          for (int k = 0; k < columnsNodes.getLength(); k++) {

            Element columns = (Element) columnsNodes.item(k);
            NodeList columnNodes = columns.getElementsByTagName("column");
            for (int l = 0; l < columnNodes.getLength(); l++) {
              Element column = (Element) columnNodes.item(l);

              String columnName = column.getElementsByTagName("name").item(0).getTextContent();
              dbMetadata = new SIARDDatabaseMetadata();
              dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
              dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
              dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE_COLUMN,columnName);
              siardatabaseMetadata.add(dbMetadata);
            }
          }

          NodeList primaryKeyNodes = table.getElementsByTagName("primaryKey");
          for (int m = 0; m < primaryKeyNodes.getLength(); m++) {
            Element primaryKey = (Element) primaryKeyNodes.item(m);

            String primaryKeyName = primaryKey.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.PRIMARY_KEY,primaryKeyName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList candidateKeyNodes = table.getElementsByTagName("candidateKey");
          for (int n = 0; n < candidateKeyNodes.getLength(); n++) {
            Element candidateKey = (Element) candidateKeyNodes.item(n);
            String candidateKeyName = candidateKey.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.CANDIDATE_KEY,candidateKeyName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList foreignKeyNodes = table.getElementsByTagName("foreignKey");
          for (int n = 0; n < foreignKeyNodes.getLength(); n++) {
            Element foreignKey = (Element) foreignKeyNodes.item(n);
            String foreignKeyName = foreignKey.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.FOREIGN_KEY,foreignKeyName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList triggerNodes = table.getElementsByTagName("trigger");
          for (int n = 0; n < triggerNodes.getLength(); n++) {
            Element trigger = (Element) triggerNodes.item(n);
            String triggerName = trigger.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TRIGGER,triggerName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList checkConstraintNodes = table.getElementsByTagName("checkConstraint");
          for (int n = 0; n < checkConstraintNodes.getLength(); n++) {
            Element constraint = (Element) checkConstraintNodes.item(n);
            String constraintName = constraint.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE,tableName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.CHECK_CONSTRAINT,constraintName);
            siardatabaseMetadata.add(dbMetadata);
          }
        }

        NodeList viewNodes = schema.getElementsByTagName("view");
        for (int j = 0; j < viewNodes.getLength(); j++) {
          Element view = (Element) viewNodes.item(j);
          String viewName = view.getElementsByTagName("name").item(0).getTextContent();
          dbMetadata = new SIARDDatabaseMetadata();
          dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
          dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW,viewName);
          siardatabaseMetadata.add(dbMetadata);

          NodeList columnsViewNodes = view.getElementsByTagName("columns");
          for (int k = 0; k < columnsViewNodes.getLength(); k++) {

            Element columns = (Element) columnsViewNodes.item(k);
            NodeList columnNodes = columns.getElementsByTagName("column");
            for (int l = 0; l < columnNodes.getLength(); l++) {
              Element column = (Element) columnNodes.item(l);

              String columnName = column.getElementsByTagName("name").item(0).getTextContent();
              dbMetadata = new SIARDDatabaseMetadata();
              dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
              dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW,viewName);
              dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW_COLUMN,columnName);
              siardatabaseMetadata.add(dbMetadata);
            }
          }
        }

        NodeList routineNodes = schema.getElementsByTagName("routine");
        for (int j = 0; j < routineNodes.getLength(); j++) {
          Element routine = (Element) routineNodes.item(j);
          String routineName = routine.getElementsByTagName("name").item(0).getTextContent();
          dbMetadata = new SIARDDatabaseMetadata();
          dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
          dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE,routineName);
          siardatabaseMetadata.add(dbMetadata);

          NodeList routineParametersNode = routine.getElementsByTagName("parameters");
          for (int k = 0; k < routineParametersNode.getLength(); k++) {
            Element parameter = (Element) routineParametersNode.item(k);
            String parameterName = parameter.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA,schemaName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE,routineName);
            dbMetadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE_PARAMETER,parameterName);
            siardatabaseMetadata.add(dbMetadata);
          }
        }
      }
    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }

    return siardatabaseMetadata;
  }

  private static List<String> getSIARDMetadata(Document document, String xpathExpression) {
    XPathFactory xPathFactory = XPathFactory.newInstance();

    XPath xpath = xPathFactory.newXPath();

    xpath.setNamespaceContext(new NamespaceContext() {
      @Override
      public Iterator getPrefixes(String arg0) {
        return null;
      }

      @Override
      public String getPrefix(String arg0) {
        return null;
      }

      @Override
      public String getNamespaceURI(String arg0) {
        if ("xs".equals(arg0)) {
          return "http://www.w3.org/2001/XMLSchema";
        }
        if ("ns".equals(arg0)) {
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    List<String> values = new ArrayList<>();

    try {
      XPathExpression expr = xpath.compile(xpathExpression);
      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
        values.add(nodes.item(i).getNodeValue());
      }
    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }

    return values;
  }

  private static Document getDocument(InputStream inputStream)
    throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    return builder.parse(inputStream);
  }
}
