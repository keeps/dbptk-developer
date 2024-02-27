/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.XMLConstants;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.structure.CandidateKey;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.Parameter;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.modules.siard.bindings.siard_2_0.CandidateKeyType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.CandidateKeysType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.CheckConstraintType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.CheckConstraintsType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ColumnType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ColumnsType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ForeignKeyType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ForeignKeysType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ParameterType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ParametersType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.PrimaryKeyType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.PrivilegeType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.PrivilegesType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ReferenceType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.RoleType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.RolesType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.RoutineType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.RoutinesType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.SchemaType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.SchemasType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.SiardArchive;
import com.databasepreservation.modules.siard.bindings.siard_2_0.TableType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.TablesType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.TriggerType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.TriggersType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.UserType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.UsersType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ViewType;
import com.databasepreservation.modules.siard.bindings.siard_2_0.ViewsType;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQL2008StandardDatatypeImporter;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQLStandardDatatypeImporter;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.utils.JodaUtils;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD20MetadataImportStrategy implements MetadataImportStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD20MetadataImportStrategy.class);
  private static final String METADATA_FILENAME = "metadata";

  private DatabaseStructure databaseStructure;
  private final MetadataPathStrategy metadataPathStrategy;
  private final ContentPathImportStrategy contentPathStrategy;
  private ModuleConfiguration moduleConfiguration;

  private SQLStandardDatatypeImporter sqlStandardDatatypeImporter;

  private int currentSchemaIndex = 1;
  private int currentTableIndex;

  private String metadataCurrentDatabaseName = "<information not available>";
  private String metadataCurrentSchemaName = "<information not available>";
  private String metadataCurrentTableName = "<information not available>";

  private Reporter reporter;

  public SIARD20MetadataImportStrategy(MetadataPathStrategy metadataPathStrategy,
    ContentPathImportStrategy contentPathImportStrategy) {
    this.metadataPathStrategy = metadataPathStrategy;
    this.contentPathStrategy = contentPathImportStrategy;
    sqlStandardDatatypeImporter = new SQL2008StandardDatatypeImporter();
  }

  @Override
  public void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container,
    ModuleConfiguration moduleConfiguration) throws ModuleException {
    this.moduleConfiguration = moduleConfiguration;
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardArchive.class.getPackage().getName(), SiardArchive.class.getClassLoader());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdStream = readStrategy.createInputStream(container,
      metadataPathStrategy.getXsdFilePath(METADATA_FILENAME));
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(METADATA_FILENAME))
        .withCause(e);
    }

    InputStream reader = null;
    SiardArchive xmlRoot;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(xsdSchema);

      reader = readStrategy.createInputStream(container, metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));
      xmlRoot = (SiardArchive) unmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      LOGGER.warn("The metadata.xml file did not pass the XML Schema validation.",
        new ModuleException().withMessage("Error while Unmarshalling JAXB with XSD").withCause(e));
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
          LOGGER.trace("problem closing reader after XMl validation failure", e1);
        }
      }
      try {
        unmarshaller = context.createUnmarshaller();
        reader = readStrategy.createInputStream(container, metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));
        xmlRoot = (SiardArchive) unmarshaller.unmarshal(reader);
      } catch (JAXBException e1) {
        throw new ModuleException().withMessage("The metadata.xml file could not be read.").withCause(e1);
      }
    } finally {
      try {
        xsdStream.close();
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOGGER.debug("Could not close xsdStream", e);
      }
    }

    databaseStructure = getDatabaseStructure(xmlRoot);
  }

  @Override
  public DatabaseStructure getDatabaseStructure() throws ModuleException {
    if (databaseStructure != null) {
      return databaseStructure;
    } else {
      throw new ModuleException().withMessage("getDatabaseStructure must not be called before loadMetadata");
    }
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    sqlStandardDatatypeImporter.setOnceReporter(reporter);
  }

  private DatabaseStructure getDatabaseStructure(SiardArchive siardArchive) throws ModuleException {
    DatabaseStructure databaseStructure = new DatabaseStructure();

    databaseStructure.setDescription(siardArchive.getDescription());
    databaseStructure.setArchiver(siardArchive.getArchiver());
    databaseStructure.setArchiverContact(siardArchive.getArchiverContact());
    databaseStructure.setDataOwner(siardArchive.getDataOwner());
    databaseStructure.setDataOriginTimespan(siardArchive.getDataOriginTimespan());
    databaseStructure.setProducerApplication(siardArchive.getProducerApplication());
    databaseStructure.setClientMachine(siardArchive.getClientMachine());
    databaseStructure.setDatabaseUser(siardArchive.getDatabaseUser());

    metadataCurrentDatabaseName = siardArchive.getDbname();
    databaseStructure.setName(metadataCurrentDatabaseName);

    databaseStructure.setArchivalDate(JodaUtils.xsDateParse(siardArchive.getArchivalDate()));
    // TODO:
    // databaseStructure.setMessageDigest(siardArchive.getMessageDigest());
    databaseStructure.setProductName(siardArchive.getDatabaseProduct());
    // databaseStructure.setProductVersion(null); //TODO: SIARD has no field for
    // product version

    databaseStructure.setUrl(siardArchive.getConnection());
    databaseStructure.setSchemas(getSchemas(siardArchive.getSchemas()));
    databaseStructure.setUsers(getUsers(siardArchive.getUsers()));
    databaseStructure.setRoles(getRoles(siardArchive.getRoles()));
    databaseStructure.setPrivileges(getPrivileges(siardArchive.getPrivileges()));

    if (contentPathStrategy instanceof SIARD2ContentPathImportStrategy) {
      SIARD2ContentPathImportStrategy siard2ContentPathImportStrategy = (SIARD2ContentPathImportStrategy) contentPathStrategy;
      siard2ContentPathImportStrategy.setMetadataLobFolder(siardArchive.getLobFolder());
    }

    return databaseStructure;
  }

  private List<PrivilegeStructure> getPrivileges(PrivilegesType privileges) {
    List<PrivilegeStructure> result = new ArrayList<PrivilegeStructure>();

    if (privileges != null && !privileges.getPrivilege().isEmpty()) {
      for (PrivilegeType privilegeType : privileges.getPrivilege()) {
        result.add(getPrivilegeStructure(privilegeType));
      }
    }

    return result;
  }

  private PrivilegeStructure getPrivilegeStructure(PrivilegeType privilegeType) {
    if (privilegeType != null) {
      PrivilegeStructure result = new PrivilegeStructure();

      result.setType(privilegeType.getType());
      result.setObject(privilegeType.getObject());
      result.setGrantor(privilegeType.getGrantor());
      result.setGrantee(privilegeType.getGrantee());
      if (privilegeType.getOption() != null) {
        result.setOption(privilegeType.getOption().value());
      }
      result.setDescription(privilegeType.getDescription());

      return result;
    } else {
      return null;
    }
  }

  private List<RoleStructure> getRoles(RolesType roles) {
    List<RoleStructure> result = new ArrayList<RoleStructure>();

    if (roles != null && !roles.getRole().isEmpty()) {
      for (RoleType roleType : roles.getRole()) {
        result.add(getRoleStructure(roleType));
      }
    }

    return result;
  }

  private RoleStructure getRoleStructure(RoleType roleType) {
    if (roleType != null) {
      RoleStructure result = new RoleStructure();

      result.setName(roleType.getName());
      result.setAdmin(roleType.getAdmin());
      result.setDescription(roleType.getDescription());

      return result;
    } else {
      return null;
    }
  }

  private List<UserStructure> getUsers(UsersType users) {
    List<UserStructure> result = new ArrayList<UserStructure>();

    if (users != null && !users.getUser().isEmpty()) {
      for (UserType userType : users.getUser()) {
        result.add(getUserStructure(userType));
      }
    }

    return result;
  }

  private UserStructure getUserStructure(UserType userType) {
    if (userType != null) {
      UserStructure result = new UserStructure();

      result.setName(userType.getName());
      result.setDescription(userType.getDescription());

      return result;
    } else {
      return null;
    }
  }

  private List<SchemaStructure> getSchemas(SchemasType schemas) throws ModuleException {
    List<SchemaStructure> result = new ArrayList<SchemaStructure>();

    if (schemas != null && !schemas.getSchema().isEmpty()) {
      for (SchemaType schema : schemas.getSchema()) {
        result.add(getSchemaStructure(schema));
      }
    }

    return result;
  }

  private SchemaStructure getSchemaStructure(SchemaType schema) throws ModuleException {
    if (schema != null) {
      SchemaStructure result = new SchemaStructure();

      metadataCurrentSchemaName = schema.getName();
      result.setName(metadataCurrentSchemaName);
      result.setDescription(schema.getDescription());
      result.setIndex(currentSchemaIndex++);
      contentPathStrategy.associateSchemaWithFolder(schema.getName(), schema.getFolder());

      // TODO: complex types: result.setTypes(getTypes(schema.getTypes()));

      currentTableIndex = 1;
      result.setTables(getTablesStructure(schema.getTables(), schema.getName()));
      result.setViews(getViews(schema.getViews()));
      result.setRoutines(getRoutines(schema.getRoutines()));

      return result;
    } else {
      return null;
    }
  }

  private List<RoutineStructure> getRoutines(RoutinesType routines) throws ModuleException {
    List<RoutineStructure> result = new ArrayList<RoutineStructure>();

    if (routines != null && !routines.getRoutine().isEmpty()) {
      for (RoutineType routineType : routines.getRoutine()) {
        result.add(getRoutineStructure(routineType));
      }
    }

    return result;
  }

  private RoutineStructure getRoutineStructure(RoutineType routineType) throws ModuleException {
    if (routineType != null) {
      RoutineStructure result = new RoutineStructure();

      metadataCurrentTableName = routineType.getName();
      result.setName(metadataCurrentTableName);
      result.setDescription(routineType.getDescription());
      result.setSource(routineType.getSource());
      result.setBody(routineType.getBody());
      result.setCharacteristic(routineType.getCharacteristic());
      result.setReturnType(routineType.getReturnType());
      // TODO: XSD has name attributes but has type ParametersType, find out if
      // this is a typo
      result.setParameters(getParameters(routineType.getAttributes()));

      return result;
    } else {
      return null;
    }
  }

  private List<Parameter> getParameters(ParametersType parameters) throws ModuleException {
    List<Parameter> result = new ArrayList<Parameter>();

    if (parameters != null && !parameters.getParameter().isEmpty()) {
      for (ParameterType parameterType : parameters.getParameter()) {
        result.add(getParameter(parameterType));
      }
    }

    return result;
  }

  private Parameter getParameter(ParameterType parameterType) throws ModuleException {
    if (parameterType != null) {
      Parameter result = new Parameter();

      result.setName(parameterType.getName());
      result.setMode(parameterType.getMode());
      result.setType(sqlStandardDatatypeImporter.getCheckedType(metadataCurrentDatabaseName, metadataCurrentSchemaName,
        metadataCurrentTableName + " (routine)", parameterType.getName() + " (parameter)", parameterType.getType(),
        parameterType.getTypeOriginal()));
      result.setDescription(parameterType.getDescription());

      // todo: deal with these fields (related to complex types)

      // this is null except if the type is a complex datatype defined in a
      // different schema
      // parameterType.getTypeSchema();

      // this is the name of the complex datatype
      // parameterType.getTypeName();

      // this is list of fields of the parameter
      // parameterType.getParameterFields();

      return result;
    } else {
      return null;
    }
  }

  private List<ViewStructure> getViews(ViewsType views) throws ModuleException {
    List<ViewStructure> result = new ArrayList<ViewStructure>();

    if (views != null && !views.getView().isEmpty()) {
      for (ViewType viewType : views.getView()) {
        result.add(getViewStructure(viewType));
      }
    }

    return result;
  }

  private ViewStructure getViewStructure(ViewType viewType) throws ModuleException {
    if (viewType != null) {
      ViewStructure result = new ViewStructure();

      result.setName(viewType.getName());
      result.setQuery(viewType.getQuery());
      result.setQueryOriginal(viewType.getQueryOriginal());
      result.setDescription(viewType.getDescription());
      result.setColumns(getColumns(viewType.getColumns(), "")); // TODO: decide
                                                                // what to put
                                                                // here as table
                                                                // name
      // TODO: result.setRows(getRows(viewType.getRows()));

      return result;
    } else {
      return null;
    }
  }

  private List<TableStructure> getTablesStructure(TablesType tables, String schemaName) throws ModuleException {
    List<TableStructure> result = new ArrayList<TableStructure>();

    if (tables != null && !tables.getTable().isEmpty()) {
      for (TableType table : tables.getTable()) {
        TableStructure obtainedTableStructure = getTableStructure(table, schemaName);
        if (moduleConfiguration.isSelectedTable(schemaName, obtainedTableStructure.getName())) {
          result.add(obtainedTableStructure);
          currentTableIndex++;
        }
      }
    }

    return result;
  }

  private TableStructure getTableStructure(TableType table, String schemaName) throws ModuleException {
    if (table != null) {
      TableStructure result = new TableStructure();

      result.setIndex(currentTableIndex);
      metadataCurrentTableName = table.getName();
      result.setName(metadataCurrentTableName);
      result.setSchema(schemaName);
      result.setDescription(table.getDescription());
      result.setId(String.format("%s.%s", result.getSchema(), result.getName()));

      contentPathStrategy.associateTableWithFolder(result.getId(), table.getFolder());

      result.setPrimaryKey(getPrimaryKey(table.getPrimaryKey()));

      result.setColumns(getColumns(table.getColumns(), result.getId()));
      result.setForeignKeys(getForeignKeys(table.getForeignKeys(), result.getId()));
      result.setCandidateKeys(getCandidateKeys(table.getCandidateKeys()));
      result.setCheckConstraints(getCheckConstraints(table.getCheckConstraints()));
      result.setTriggers(getTriggers(table.getTriggers()));
      result.setRows(table.getRows().longValue());

      return result;
    } else {
      return null;
    }
  }

  private List<Trigger> getTriggers(TriggersType triggers) {
    List<Trigger> result = new ArrayList<Trigger>();

    if (triggers != null && !triggers.getTrigger().isEmpty()) {
      for (TriggerType triggerType : triggers.getTrigger()) {
        result.add(getTrigger(triggerType));
      }
    }

    return result;
  }

  private Trigger getTrigger(TriggerType triggerType) {
    if (triggerType != null) {
      Trigger result = new Trigger();

      result.setName(XMLUtils.decode(triggerType.getName()));
      result.setActionTime(triggerType.getActionTime().value());
      result.setTriggerEvent(XMLUtils.decode(triggerType.getTriggerEvent()));
      result.setAliasList(triggerType.getAliasList());
      result.setTriggeredAction(XMLUtils.decode(triggerType.getTriggeredAction()));
      result.setDescription(triggerType.getDescription());

      return result;
    } else {
      return null;
    }
  }

  private List<CheckConstraint> getCheckConstraints(CheckConstraintsType checkConstraints) {
    List<CheckConstraint> result = new ArrayList<CheckConstraint>();

    if (checkConstraints != null && !checkConstraints.getCheckConstraint().isEmpty()) {
      for (CheckConstraintType checkConstraintType : checkConstraints.getCheckConstraint()) {
        result.add(getCheckConstraint(checkConstraintType));
      }
    }

    return result;
  }

  private CheckConstraint getCheckConstraint(CheckConstraintType checkConstraintType) {
    if (checkConstraintType != null) {
      CheckConstraint result = new CheckConstraint();

      result.setName(checkConstraintType.getName());
      result.setCondition(checkConstraintType.getCondition());
      result.setDescription(checkConstraintType.getDescription());

      return result;
    } else {
      return null;
    }
  }

  private List<CandidateKey> getCandidateKeys(CandidateKeysType candidateKeys) {
    List<CandidateKey> result = new ArrayList<CandidateKey>();

    if (candidateKeys != null && !candidateKeys.getCandidateKey().isEmpty()) {
      for (CandidateKeyType candidateKeyType : candidateKeys.getCandidateKey()) {
        result.add(getCandidateKey(candidateKeyType));
      }
    }

    return result;
  }

  private CandidateKey getCandidateKey(CandidateKeyType candidateKeyType) {
    if (candidateKeyType != null) {
      CandidateKey result = new CandidateKey();

      result.setName(candidateKeyType.getName());
      result.setDescription(candidateKeyType.getDescription());
      result.setColumns(candidateKeyType.getColumn());

      return result;
    } else {
      return null;
    }
  }

  private List<ForeignKey> getForeignKeys(ForeignKeysType foreignKeys, String tableId) {
    List<ForeignKey> result = new ArrayList<ForeignKey>();

    if (foreignKeys != null && !foreignKeys.getForeignKey().isEmpty()) {
      for (ForeignKeyType foreignKey : foreignKeys.getForeignKey()) {
        result.add(getForeignKey(foreignKey, tableId));
      }
    }

    return result;
  }

  private ForeignKey getForeignKey(ForeignKeyType foreignKey, String tableId) {
    if (foreignKey != null) {
      ForeignKey result = new ForeignKey();

      result.setId(String.format("%s.%s", tableId, foreignKey.getName()));
      result.setName(foreignKey.getName());
      result.setReferencedSchema(foreignKey.getReferencedSchema());
      result.setReferencedTable(foreignKey.getReferencedTable());
      if (foreignKey.getMatchType() != null) {
        result.setMatchType(foreignKey.getMatchType().value());
      }
      result.setDeleteAction(foreignKey.getDeleteAction());
      result.setUpdateAction(foreignKey.getUpdateAction());
      result.setDescription(foreignKey.getDescription());

      result.setReferences(getReferences(foreignKey.getReference()));

      return result;
    } else {
      return null;
    }
  }

  private List<Reference> getReferences(List<ReferenceType> reference) {
    List<Reference> result = new ArrayList<Reference>();

    if (reference != null && !reference.isEmpty()) {
      for (ReferenceType referenceType : reference) {
        result.add(getReference(referenceType));
      }
    }

    return result;
  }

  private Reference getReference(ReferenceType referenceType) {
    if (referenceType != null) {
      Reference result = new Reference();

      result.setColumn(referenceType.getColumn());
      result.setReferenced(referenceType.getReferenced());

      return result;
    } else {
      return null;
    }
  }

  private List<ColumnStructure> getColumns(ColumnsType columns, String tableId) throws ModuleException {
    List<ColumnStructure> result = new ArrayList<ColumnStructure>();

    if (columns != null && !columns.getColumn().isEmpty()) {
      for (ColumnType column : columns.getColumn()) {
        result.add(getColumnStructure(column, tableId));
      }
    }

    return result;
  }

  private ColumnStructure getColumnStructure(ColumnType column, String tableId) throws ModuleException {
    if (column != null) {
      ColumnStructure result = new ColumnStructure();

      result.setName(column.getName());
      result.setId(tableId + "." + result.getName());

      String lobFolder = column.getFolder();
      if (StringUtils.isBlank(lobFolder)) {
        lobFolder = column.getLobFolder();
      }

      contentPathStrategy.associateColumnWithFolder(result.getId(), column.getFolder());

      result.setType(sqlStandardDatatypeImporter.getCheckedType(metadataCurrentDatabaseName, metadataCurrentSchemaName,
        metadataCurrentTableName, column.getName(), column.getType(), column.getTypeOriginal()));

      result.setNillable(column.isNullable());
      result.setDefaultValue(column.getDefaultValue());
      result.setDescription(column.getDescription());

      // todo: deal with these fields
      // column.getLobFolder();
      // column.getTypeSchema();
      // column.getTypeName();
      // column.getFields();
      // column.getMimeType();

      return result;
    } else {
      return null;
    }
  }

  private PrimaryKey getPrimaryKey(PrimaryKeyType primaryKey) {
    if (primaryKey != null) {
      PrimaryKey result = new PrimaryKey();

      result.setName(primaryKey.getName());
      result.setDescription(primaryKey.getDescription());
      result.setColumnNames(primaryKey.getColumn());

      return result;
    } else {
      return null;
    }
  }
}
