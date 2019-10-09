/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.update;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.databasepreservation.modules.siard.out.path.SIARD1ContentPathExportStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.metadata.SIARDDatabaseMetadata;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD1MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD20MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD21MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD1ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD1MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD20MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD21MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.update.MetadataUpdateStrategy;
import com.databasepreservation.modules.siard.out.update.UpdateStrategy;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDEditModule implements EditModule {
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private MetadataImportStrategy metadataImportStrategy;

  private Reporter reporter;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDEditModule.class);

  private static final String METADATA_FILENAME = "metadata";

  /**
   * Constructor used to initialize required objects to get an edit import module
   * for SIARD 2 (all minor versions)
   *
   * @param siardPackagePath
   *          Path to the main SIARD file (file with extension .siard)
   */
  public SIARDEditModule(Path siardPackagePath) {
    Path siardPackageNormalizedPath = siardPackagePath.toAbsolutePath().normalize();
    mainContainer = new SIARDArchiveContainer(siardPackageNormalizedPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new ZipAndFolderReadStrategy(mainContainer);

    // identify version before creating metadata import strategy instance
    try {
      readStrategy.setup(mainContainer);
    } catch (ModuleException e) {
      LOGGER.debug("Problem setting up container", e);
    }

    MetadataPathStrategy metadataPathStrategy = new SIARD2MetadataPathStrategy();
    ContentPathImportStrategy contentPathStrategy = new SIARD2ContentPathImportStrategy();

    switch (mainContainer.getVersion()) {
      case V2_0:
        metadataImportStrategy = new SIARD20MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V2_1:
        metadataImportStrategy = new SIARD21MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V1_0:
        metadataImportStrategy = new SIARD1MetadataImportStrategy(new SIARD1MetadataPathStrategy(),
          new SIARD1ContentPathImportStrategy());
        break;
      case DK:
      default:
        metadataImportStrategy = null;
    }
  }

  /**
   * Gets a <code>DatabaseStructure</code> with all the metadata imported from the
   * SIARD archive.
   *
   * @return A <code>DatabaseStructure</code>
   * @throws NullPointerException
   *          If the SIARD archive version were not 2.0 or 2.1
   * @throws ModuleException
   *          Generic module exception
   */
  @Override
  public DatabaseStructure getMetadata() throws ModuleException {
    ModuleSettings moduleSettings = new ModuleSettings();

    LOGGER.info("Importing SIARD version {}", mainContainer.getVersion().getDisplayName());
    DatabaseStructure dbStructure;

    try {
      metadataImportStrategy.loadMetadata(readStrategy, mainContainer, moduleSettings);

      dbStructure = metadataImportStrategy.getDatabaseStructure();
    } catch (NullPointerException e) {
      throw new ModuleException().withMessage("Metadata editing only supports SIARD version 1, 2.0 and 2.1").withCause(e);
    } finally {
      readStrategy.finish(mainContainer);
    }
    return dbStructure;
  }

  @Override
  public String getSIARDVersion() {
    return mainContainer.getVersion().getDisplayName();
  }

  /**
   * @param dbStructure The {@link DatabaseStructure} with the updated values.
   * @throws ModuleException
   *          Generic module exception
   */
  @Override
  public void updateMetadata(DatabaseStructure dbStructure) throws ModuleException {

    MetadataPathStrategy metadataPathStrategy = new SIARD2MetadataPathStrategy();

    SIARD2ContentPathExportStrategy contentPathExportStrategy = new SIARD2ContentPathExportStrategy();

    UpdateStrategy updateStrategy = new MetadataUpdateStrategy();

    switch (mainContainer.getVersion()) {
      case V2_0:
        SIARD20MetadataExportStrategy metadata20ExportStrategy = new SIARD20MetadataExportStrategy(metadataPathStrategy,
          contentPathExportStrategy, false);
        metadata20ExportStrategy.setOnceReporter(reporter);
        metadata20ExportStrategy.updateMetadataXML(dbStructure, mainContainer, updateStrategy);
        break;
      case V2_1:
        SIARD21MetadataExportStrategy metadata21ExportStrategy = new SIARD21MetadataExportStrategy(metadataPathStrategy,
          contentPathExportStrategy, false);
        metadata21ExportStrategy.setOnceReporter(reporter);
        metadata21ExportStrategy.updateMetadataXML(dbStructure, mainContainer, updateStrategy);
        break;
      case V1_0:
        SIARD1MetadataExportStrategy metadata1ExportStrategy = new SIARD1MetadataExportStrategy(
          new SIARD1MetadataPathStrategy(), new SIARD1ContentPathExportStrategy());
        metadata1ExportStrategy.setOnceReporter(reporter);
        metadata1ExportStrategy.updateMetadataXML(dbStructure, mainContainer, updateStrategy);
        break;
      case DK:
      default:
    }
  }


  /**
   * @return A list of <code>SIARDDatabaseMetadata</code>
   * @throws ModuleException
   *          Generic module exception
   */
  @Override
  public List<SIARDDatabaseMetadata> getDescriptiveSIARDMetadataKeys() throws ModuleException {
    SIARD2MetadataPathStrategy siard2MetadataPathStrategy = new SIARD2MetadataPathStrategy();
    ZipReadStrategy zipReadStrategy = new ZipReadStrategy();
    zipReadStrategy.setup(mainContainer);

    List<SIARDDatabaseMetadata> descriptiveMetadata = new ArrayList<>();

    try (InputStream XSDStream = zipReadStrategy.createInputStream(mainContainer,
      siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))) {
      Document doc = getDocument(XSDStream);
      String xpathExpression = "//xs:element[@name='siardArchive']/xs:complexType/xs:sequence/xs:element/@name";

      ArrayList<String> ignoredMetadataKeys = new ArrayList<>();
      ignoredMetadataKeys.add("lobFolder");
      ignoredMetadataKeys.add("schemas");
      ignoredMetadataKeys.add("users");
      ignoredMetadataKeys.add("roles");
      ignoredMetadataKeys.add("privileges");

      descriptiveMetadata.addAll(getSIARDMetadata(doc, xpathExpression, ignoredMetadataKeys));

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
      String xpathExpressionPrivileges = "/ns:siardArchive/ns:privileges/ns:privilege";

      List<SIARDDatabaseMetadata> schemaMetadata = getSchemaMetadata(doc, xpathExpressionSchemas);
      List<SIARDDatabaseMetadata> usersMetadata = getUsersMetadata(doc, xpathExpressionUsers);
      List<SIARDDatabaseMetadata> rolesMetadata = getRolesMetadata(doc, xpathExpressionRoles);
      List<SIARDDatabaseMetadata> privilegesMetadata = getPrivilegesMetadata(doc, xpathExpressionPrivileges);

      SIARDDatabaseMetadataKeys.addAll(schemaMetadata);
      SIARDDatabaseMetadataKeys.addAll(usersMetadata);
      SIARDDatabaseMetadataKeys.addAll(rolesMetadata);
      SIARDDatabaseMetadataKeys.addAll(privilegesMetadata);

    } catch (ParserConfigurationException e) {
      throw new ModuleException()
        .withMessage("Error parsing the XML file: " + siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XML file: " + siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Error open the XML file: " + siard2MetadataPathStrategy.getXmlFilePath(METADATA_FILENAME))
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
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }

  // Auxiliary Internal Methods

  private List<SIARDDatabaseMetadata> getUsersMetadata(Document document, String xpathExpression)
    throws ModuleException {
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = setXPathForXML(xpath, mainContainer.getVersion());

    List<SIARDDatabaseMetadata> metadata = new ArrayList<>();

    try {

      XPathExpression expr = xpath.compile(xpathExpression);

      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
        SIARDDatabaseMetadata dbMetadata = new SIARDDatabaseMetadata();
        dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.USER, nodes.item(i).getNodeValue());
        metadata.add(dbMetadata);
      }

    } catch (XPathExpressionException e) {
      throw new ModuleException().withMessage("Error on xpath expression: " + xpathExpression).withCause(e);
    }

    return metadata;
  }

  private List<SIARDDatabaseMetadata> getRolesMetadata(Document document, String xpathExpression)
    throws ModuleException {
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = setXPathForXML(xpath, mainContainer.getVersion());

    List<SIARDDatabaseMetadata> metadata = new ArrayList<>();

    try {

      XPathExpression expr = xpath.compile(xpathExpression);

      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
        SIARDDatabaseMetadata dbMetadata = new SIARDDatabaseMetadata();
        dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROLE, nodes.item(i).getNodeValue());
        metadata.add(dbMetadata);
      }

    } catch (XPathExpressionException e) {
      throw new ModuleException().withMessage("Error on xpath expression: " + xpathExpression).withCause(e);
    }

    return metadata;
  }

  private List<SIARDDatabaseMetadata> getPrivilegesMetadata(Document document, String xpathExpression)
      throws ModuleException {
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = setXPathForXML(xpath, mainContainer.getVersion());

    List<SIARDDatabaseMetadata> metadata = new ArrayList<>();

    try {

      XPathExpression expr = xpath.compile(xpathExpression);

      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
          Element priv = (Element) nodes.item(i);

        PrivilegeStructure privilege = new PrivilegeStructure();
        privilege.setType(priv.getElementsByTagName("type").item(0).getTextContent());
        privilege.setObject(priv.getElementsByTagName("object").item(0).getTextContent());
        privilege.setGrantor(priv.getElementsByTagName("grantor").item(0).getTextContent());
        privilege.setGrantee(priv.getElementsByTagName("grantee").item(0).getTextContent());

        SIARDDatabaseMetadata dbMetadata = new SIARDDatabaseMetadata();
        dbMetadata.setPrivilege(privilege);
        metadata.add(dbMetadata);
      }

    } catch (XPathExpressionException e) {
      throw new ModuleException().withMessage("Error on xpath expression: " + xpathExpression).withCause(e);
    }

    return metadata;
  }


  private List<SIARDDatabaseMetadata> getSchemaMetadata(Document document, String xpathExpression)
    throws ModuleException {
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = setXPathForXML(xpath, mainContainer.getVersion());

    List<SIARDDatabaseMetadata> siardatabaseMetadata = new ArrayList<>();

    try {
      XPathExpression expr = xpath.compile(xpathExpression);

      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);

        String schemaName = schema.getElementsByTagName("name").item(0).getTextContent();
        SIARDDatabaseMetadata dbMetadata = new SIARDDatabaseMetadata();
        dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
        siardatabaseMetadata.add(dbMetadata);

        NodeList tableNodes = schema.getElementsByTagName("table");
        for (int j = 0; j < tableNodes.getLength(); j++) {
          Element table = (Element) tableNodes.item(j);
          String tableName = table.getElementsByTagName("name").item(0).getTextContent();
          dbMetadata = new SIARDDatabaseMetadata();
          dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
          siardatabaseMetadata.add(dbMetadata);

          NodeList columnsNodes = table.getElementsByTagName("columns");
          for (int k = 0; k < columnsNodes.getLength(); k++) {

            Element columns = (Element) columnsNodes.item(k);
            NodeList columnNodes = columns.getElementsByTagName("column");
            for (int l = 0; l < columnNodes.getLength(); l++) {
              Element column = (Element) columnNodes.item(l);

              String columnName = column.getElementsByTagName("name").item(0).getTextContent();
              dbMetadata = new SIARDDatabaseMetadata();
              dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
              dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
              dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE_COLUMN, columnName);
              siardatabaseMetadata.add(dbMetadata);
            }
          }

          NodeList primaryKeyNodes = table.getElementsByTagName("primaryKey");
          for (int m = 0; m < primaryKeyNodes.getLength(); m++) {
            Element primaryKey = (Element) primaryKeyNodes.item(m);

            String primaryKeyName = primaryKey.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.PRIMARY_KEY, primaryKeyName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList candidateKeyNodes = table.getElementsByTagName("candidateKey");
          for (int n = 0; n < candidateKeyNodes.getLength(); n++) {
            Element candidateKey = (Element) candidateKeyNodes.item(n);
            String candidateKeyName = candidateKey.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.CANDIDATE_KEY, candidateKeyName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList foreignKeyNodes = table.getElementsByTagName("foreignKey");
          for (int n = 0; n < foreignKeyNodes.getLength(); n++) {
            Element foreignKey = (Element) foreignKeyNodes.item(n);
            String foreignKeyName = foreignKey.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.FOREIGN_KEY, foreignKeyName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList triggerNodes = table.getElementsByTagName("trigger");
          for (int n = 0; n < triggerNodes.getLength(); n++) {
            Element trigger = (Element) triggerNodes.item(n);
            String triggerName = trigger.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TRIGGER, triggerName);
            siardatabaseMetadata.add(dbMetadata);
          }

          NodeList checkConstraintNodes = table.getElementsByTagName("checkConstraint");
          for (int n = 0; n < checkConstraintNodes.getLength(); n++) {
            Element constraint = (Element) checkConstraintNodes.item(n);
            String constraintName = constraint.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.CHECK_CONSTRAINT, constraintName);
            siardatabaseMetadata.add(dbMetadata);
          }
        }

        NodeList viewNodes = schema.getElementsByTagName("view");
        for (int j = 0; j < viewNodes.getLength(); j++) {
          Element view = (Element) viewNodes.item(j);
          String viewName = view.getElementsByTagName("name").item(0).getTextContent();
          dbMetadata = new SIARDDatabaseMetadata();
          dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.VIEW, viewName);
          siardatabaseMetadata.add(dbMetadata);

          NodeList columnsViewNodes = view.getElementsByTagName("columns");
          for (int k = 0; k < columnsViewNodes.getLength(); k++) {

            Element columns = (Element) columnsViewNodes.item(k);
            NodeList columnNodes = columns.getElementsByTagName("column");
            for (int l = 0; l < columnNodes.getLength(); l++) {
              Element column = (Element) columnNodes.item(l);

              String columnName = column.getElementsByTagName("name").item(0).getTextContent();
              dbMetadata = new SIARDDatabaseMetadata();
              dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
              dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.VIEW, viewName);
              dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.VIEW_COLUMN, columnName);
              siardatabaseMetadata.add(dbMetadata);
            }
          }
        }

        NodeList routineNodes = schema.getElementsByTagName("routine");
        for (int j = 0; j < routineNodes.getLength(); j++) {
          Element routine = (Element) routineNodes.item(j);
          String routineName = routine.getElementsByTagName("name").item(0).getTextContent();
          dbMetadata = new SIARDDatabaseMetadata();
          dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROUTINE, routineName);
          siardatabaseMetadata.add(dbMetadata);

          NodeList routineParametersNode = routine.getElementsByTagName("parameters");
          for (int k = 0; k < routineParametersNode.getLength(); k++) {
            Element parameter = (Element) routineParametersNode.item(k);
            String parameterName = parameter.getElementsByTagName("name").item(0).getTextContent();
            dbMetadata = new SIARDDatabaseMetadata();
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROUTINE, routineName);
            dbMetadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROUTINE_PARAMETER, parameterName);
            siardatabaseMetadata.add(dbMetadata);
          }
        }
      }
    } catch (XPathExpressionException e) {
      throw new ModuleException().withMessage("Error on xpath expression: " + xpathExpression).withCause(e);
    }

    return siardatabaseMetadata;
  }

  private static List<SIARDDatabaseMetadata> getSIARDMetadata(Document document, String xpathExpression,
    List<String> ignoredMetadataKeys) {
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

    List<SIARDDatabaseMetadata> values = new ArrayList<>();

    try {
      XPathExpression expr = xpath.compile(xpathExpression);
      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      SIARDDatabaseMetadata metadata;

      for (int i = 0; i < nodes.getLength(); i++) {
        String metadataKey = nodes.item(i).getNodeValue();
        if (ignoredMetadataKeys != null) {
          if (!ignoredMetadataKeys.contains(metadataKey)) {
            metadata = new SIARDDatabaseMetadata(metadataKey, "");
            values.add(metadata);
          }
        }
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

  private static XPath setXPathForXML(XPath xPath, final SIARDConstants.SiardVersion siardVersion) {
    xPath.setNamespaceContext(new NamespaceContext() {
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
          if (siardVersion.equals(SIARDConstants.SiardVersion.V2_0)) {
            return "http://www.bar.admin.ch/xmlns/siard/2.0/metadata.xsd";
          } else if (siardVersion.equals(SIARDConstants.SiardVersion.V2_1)) {
            return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
          } else {
            return "http://www.bar.admin.ch/xmlns/siard/1.0/metadata.xsd";
          }
        }
        return null;
      }
    });

    return xPath;
  }
}
