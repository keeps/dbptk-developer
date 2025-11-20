/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.reporters.Reporter;
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
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.SIARDHelper;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ActionTimeType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.AttributeType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.AttributesType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.CandidateKeysType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.CategoryType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.CheckConstraintType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.CheckConstraintsType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ColumnType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ColumnsType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ForeignKeyType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ForeignKeysType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.MatchTypeType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ParameterType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ParametersType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.PrivOptionType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.PrivilegeType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.PrivilegesType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ReferenceType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ReferentialActionType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.RoleType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.RolesType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.RoutineType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.RoutinesType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.SchemaType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.SchemasType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.SiardArchive;
import com.databasepreservation.modules.siard.bindings.siard_2_2.TableType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.TablesType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.TriggerType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.TriggersType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.TypeType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.TypesType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.UniqueKeyType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.UserType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.UsersType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ViewType;
import com.databasepreservation.modules.siard.bindings.siard_2_2.ViewsType;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.content.Sql2008toXSDType;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.update.UpdateStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.JodaUtils;
import com.databasepreservation.utils.XMLUtils;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD22MetadataExportStrategy implements MetadataExportStrategy {
  private static final String ENCODING = "UTF-8";
  private static final String METADATA_FILENAME = "metadata";
  private static final String METADATA_RESOURCE_FILENAME = "siard2-2-metadata";
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD22MetadataExportStrategy.class);
  private final ContentPathExportStrategy contentPathStrategy;
  private final MetadataPathStrategy metadataPathStrategy;
  private final boolean savingLobsExternally;

  private Reporter reporter;

  public SIARD22MetadataExportStrategy(MetadataPathStrategy metadataPathStrategy, ContentPathExportStrategy paths,
    boolean savingLobsExternally) {
    this.contentPathStrategy = paths;
    this.metadataPathStrategy = metadataPathStrategy;
    this.savingLobsExternally = savingLobsExternally;
  }

  public void updateMetadataXML(DatabaseStructure dbStructure, SIARDArchiveContainer container,
    UpdateStrategy updateStrategy) throws ModuleException {

    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardArchive.class.getPackage().getName(), SiardArchive.class.getClassLoader());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(
        SiardArchive.class.getResourceAsStream(metadataPathStrategy.getXsdResourcePath(METADATA_RESOURCE_FILENAME))));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("XSD file has errors: " + metadataPathStrategy.getXsdResourcePath(METADATA_RESOURCE_FILENAME))
        .withCause(e);
    }

    SiardArchive xmlroot = jaxbSiardArchive(dbStructure);
    Marshaller m;

    try {
      m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      m.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
      m.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
        "http://www.bar.admin.ch/xmlns/siard/" + container.getVersion().getNamespace() + "/metadata.xsd metadata.xsd");

      m.setSchema(xsdSchema);

      OutputStream metadataOutputStream = updateStrategy.createOutputStream();
      m.marshal(xmlroot, metadataOutputStream);

      updateStrategy.close();

      updateStrategy.updateSIARDArchive(container, metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));

    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error while Marshalling JAXB").withCause(e);
    }
  }

  @Override
  public void writeMetadataXML(DatabaseStructure dbStructure, SIARDArchiveContainer container,
    WriteStrategy writeStrategy) throws ModuleException {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardArchive.class.getPackage().getName(), SiardArchive.class.getClassLoader());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(
        SiardArchive.class.getResourceAsStream(metadataPathStrategy.getXsdResourcePath(METADATA_RESOURCE_FILENAME))));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("XSD file has errors: " + metadataPathStrategy.getXsdResourcePath(METADATA_RESOURCE_FILENAME))
        .withCause(e);
    }

    removeViewsWithoutColumns(dbStructure);
    setLobFolder(dbStructure, container, savingLobsExternally);
    SiardArchive xmlroot = jaxbSiardArchive(dbStructure);
    Marshaller m;
    try {
      m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      m.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
      m.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
        "http://www.bar.admin.ch/xmlns/siard/" + container.getVersion().getNamespace() + "/metadata.xsd metadata.xsd");

      // m.marshal(xmlroot, System.out);

      m.setSchema(xsdSchema);
      OutputStream writer = writeStrategy.createOutputStream(container,
        metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));
      m.marshal(xmlroot, writer);
      writer.close();
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error while Marshalling JAXB").withCause(e);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error while closing the data writer").withCause(e);
    }

    // create subfolder header/siardversion/2.2
    OutputStream writer = writeStrategy.createOutputStream(container, "header/siardversion/2.2/");
    try {
      writer.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error while closing the data writer").withCause(e);
    }
  }

  @Override
  public void writeMetadataXSD(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
    WriteStrategy writeStrategy) throws ModuleException {
    // prepare to write
    OutputStream out = writeStrategy.createOutputStream(container,
      metadataPathStrategy.getXsdFilePath(METADATA_FILENAME));

    // prepare to read
    InputStream in = SiardArchive.class
      .getResourceAsStream(metadataPathStrategy.getXsdResourcePath(METADATA_RESOURCE_FILENAME));

    // read everything from reader into writer
    try {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write "
        + metadataPathStrategy.getXsdFilePath(METADATA_FILENAME) + " in container " + container.toString())
        .withCause(e);
    }

    // close input
    try {
      in.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not close stream").withCause(e);
    }

    // close output
    try {
      out.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not close stream").withCause(e);
    }
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  private void setLobFolder(DatabaseStructure dbStructure, SIARDArchiveContainer container,
    boolean savingLobsExternally) {
    if (!savingLobsExternally) {
      dbStructure.setLobFolder("content");
    } else {
      dbStructure.setLobFolder("../" + container.getPath().toAbsolutePath().getFileName() + "_lobs");
    }
  }

  private SiardArchive jaxbSiardArchive(DatabaseStructure dbStructure) throws ModuleException {
    SiardArchive siardArchive = new SiardArchive();
    siardArchive.setArchivalDate(JodaUtils.xsDateFormat(dbStructure.getArchivalDate()));

    siardArchive.setVersion("2.2");

    if (StringUtils.isNotBlank(dbStructure.getName())) {
      siardArchive.setDbname(dbStructure.getName());
    } else {
      throw new ModuleException().withMessage("Error while exporting structure: dbname cannot be blank");
    }

    if (StringUtils.isNotBlank(dbStructure.getDescription())) {
      siardArchive.setDescription(XMLUtils.encode(dbStructure.getDescription()));
    }

    if (StringUtils.isNotBlank(dbStructure.getArchiver())) {
      siardArchive.setArchiver(dbStructure.getArchiver());
    }

    if (StringUtils.isNotBlank(dbStructure.getArchiverContact())) {
      siardArchive.setArchiverContact(dbStructure.getArchiverContact());
    }

    if (StringUtils.isNotBlank(dbStructure.getDataOwner())) {
      siardArchive.setDataOwner(dbStructure.getDataOwner());
    } else {
      throw new ModuleException().withMessage("Error while exporting structure: data owner cannot be blank");
    }

    if (StringUtils.isNotBlank(dbStructure.getDataOriginTimespan())) {
      siardArchive.setDataOriginTimespan(dbStructure.getDataOriginTimespan());
    } else {
      throw new ModuleException().withMessage("Error while exporting structure: data origin timestamp cannot be blank");
    }

    if (StringUtils.isNotBlank(dbStructure.getProducerApplication())) {
      siardArchive.setProducerApplication(dbStructure.getProducerApplication());
    }

    if (dbStructure.getArchivalDate() != null) {
      siardArchive.setArchivalDate(JodaUtils.xsDateFormat(dbStructure.getArchivalDate()));
    }

    // TODO: set message digest
    // siardArchive.setMessageDigest("");

    if (StringUtils.isNotBlank(dbStructure.getProductName())) {
      if (StringUtils.isBlank(dbStructure.getProductVersion())) {
        siardArchive.setDatabaseProduct(dbStructure.getProductName());
      } else {
        siardArchive.setDatabaseProduct(dbStructure.getProductName() + " " + dbStructure.getProductVersion());
      }
    }

    if (StringUtils.isNotBlank(dbStructure.getUrl())) {
      siardArchive.setConnection(dbStructure.getUrl());
    }

    if (StringUtils.isNotBlank(dbStructure.getDatabaseUser())) {
      siardArchive.setDatabaseUser(dbStructure.getDatabaseUser());
    }

    if (StringUtils.isNotBlank(dbStructure.getClientMachine())) {
      siardArchive.setClientMachine(dbStructure.getClientMachine());
    }

    siardArchive.setSchemas(jaxbSchemasType(dbStructure.getSchemas()));
    siardArchive.setUsers(jaxbUsersType(dbStructure.getUsers()));
    siardArchive.setRoles(jaxbRolesType(dbStructure.getRoles()));
    siardArchive.setPrivileges(jaxbPrivilegesType(dbStructure.getPrivileges()));

    siardArchive.setLobFolder(dbStructure.getLobFolder());

    return siardArchive;
  }

  private void removeViewsWithoutColumns(DatabaseStructure databaseStructure) {
    for (SchemaStructure s : databaseStructure.getSchemas()) {
      List<ViewStructure> list = new ArrayList<>();
      for (ViewStructure v : s.getViews()) {
        if (v.getColumns() != null) {
          list.add(v);
        } else {
          reporter.viewWithNullColumns(v.getName(), s.getName());
        }
      }
      s.setViews(list);
    }
  }

  private PrivilegesType jaxbPrivilegesType(List<PrivilegeStructure> privileges) throws ModuleException {
    if (privileges != null && !privileges.isEmpty()) {
      PrivilegesType privilegesType = new PrivilegesType();
      for (PrivilegeStructure privilege : privileges) {
        privilegesType.getPrivilege().add(jaxbPrivilegeType(privilege));
      }
      return privilegesType;
    } else {
      return null;
    }
  }

  private PrivilegeType jaxbPrivilegeType(PrivilegeStructure privilege) throws ModuleException {
    PrivilegeType privilegeType = new PrivilegeType();

    if (StringUtils.isNotEmpty(privilege.getType())) {
      privilegeType.setType(privilege.getType());
    } else {
      throw new ModuleException().withMessage("Error while exporting users structure: privilege type cannot be blank");
    }

    if (StringUtils.isNotEmpty(privilege.getObject())) {
      privilegeType.setObject(privilege.getObject());
    } else {
      privilegeType.setObject("unknown object");
      // LOGGER.warn("Could not export privilege object");
      // TODO: check in which circumstances this happens
      throw new ModuleException()
        .withMessage("Error while exporting users structure: privilege object cannot be blank");
    }

    if (StringUtils.isNotBlank(privilege.getGrantor())) {
      privilegeType.setGrantor(privilege.getGrantor());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting users structure: privilege grantor cannot be blank");
    }

    if (StringUtils.isNotBlank(privilege.getGrantee())) {
      privilegeType.setGrantee(privilege.getGrantee());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting users structure: privilege grantee cannot be blank");
    }

    if (StringUtils.isNotBlank(privilege.getOption()) && SIARDHelper.isValidOption(privilege.getOption())) {
      privilegeType.setOption(PrivOptionType.fromValue(privilege.getOption()));
    }

    if (StringUtils.isNotBlank(privilege.getDescription())) {
      privilegeType.setDescription(XMLUtils.encode(privilege.getDescription()));
    }

    return privilegeType;
  }

  private RolesType jaxbRolesType(List<RoleStructure> roles) throws ModuleException {
    if (roles != null && !roles.isEmpty()) {
      RolesType rolesType = new RolesType();
      for (RoleStructure role : roles) {
        rolesType.getRole().add(jaxbRoleType(role));
      }
      return rolesType;
    } else {
      return null;
    }
  }

  private RoleType jaxbRoleType(RoleStructure role) throws ModuleException {
    RoleType roleType = new RoleType();

    if (StringUtils.isNotBlank(role.getName())) {
      roleType.setName(role.getName());
    } else {
      throw new ModuleException().withMessage("Error while exporting users structure: user name cannot be blank");
    }

    if (role.getAdmin() != null) {
      roleType.setAdmin(role.getAdmin());
    } else {
      // TODO: check in which circumstances this happens
      throw new ModuleException().withMessage("Error while exporting users structure: role admin cannot be null");
    }

    if (StringUtils.isNotBlank(role.getDescription())) {
      roleType.setDescription(XMLUtils.encode(role.getDescription()));
    }

    return roleType;
  }

  private UsersType jaxbUsersType(List<UserStructure> users) throws ModuleException {
    UsersType usersType = new UsersType();
    if (users != null && !users.isEmpty()) {
      for (UserStructure user : users) {
        usersType.getUser().add(jaxbUserType(user));
      }
    }
    return usersType;
  }

  private UserType jaxbUserType(UserStructure user) throws ModuleException {
    UserType userType = new UserType();

    if (StringUtils.isNotBlank(user.getName())) {
      userType.setName(user.getName());
    } else {
      throw new ModuleException().withMessage("Error while exporting users structure: user name cannot be blank");
    }

    if (StringUtils.isNotBlank(user.getDescription())) {
      userType.setDescription(XMLUtils.encode(user.getDescription()));
    }

    return userType;
  }

  private SchemasType jaxbSchemasType(List<SchemaStructure> schemas) throws ModuleException {
    if (schemas != null && !schemas.isEmpty()) {
      SchemasType schemasType = new SchemasType();
      for (SchemaStructure schema : schemas) {
        if (schema.getTables().isEmpty()) {
          LOGGER.warn("Schema {} was not exported because it does not contain tables.", schema.getName());
        } else {
          schemasType.getSchema().add(jaxbSchemaType(schema));
        }
      }
      return schemasType;
    } else {
      return null;
    }
  }

  private SchemaType jaxbSchemaType(SchemaStructure schema) throws ModuleException {
    SchemaType schemaType = new SchemaType();

    if (StringUtils.isNotBlank(schema.getName())) {
      schemaType.setName(schema.getName());
      if (StringUtils.isNotBlank(schema.getFolder())) {
        schemaType.setFolder(schema.getFolder());
      } else {
        schemaType.setFolder(contentPathStrategy.getSchemaFolderName(schema.getIndex()));
      }
    } else {
      throw new ModuleException().withMessage("Error while exporting schema structure: schema name cannot be blank");
    }

    if (StringUtils.isNotBlank(schema.getDescription())) {
      schemaType.setDescription(XMLUtils.encode(schema.getDescription()));
    }

    schemaType.setTables(jaxbTablesType(schema, schema.getTables()));
    schemaType.setViews(jaxbViewsType(schema.getViews()));
    schemaType.setRoutines(jaxbRoutinesType(schema.getName(), schema.getRoutines()));
    schemaType.setTypes(jaxbTypesType(schema.getUserDefinedTypesComposed()));

    return schemaType;
  }

  private TypesType jaxbTypesType(List<ComposedTypeStructure> userDefinedTypes) throws ModuleException {
    if (userDefinedTypes != null && !userDefinedTypes.isEmpty()) {
      TypesType typesType = new TypesType();
      for (ComposedTypeStructure userDefinedType : userDefinedTypes) {
        typesType.getType().add(jaxbTypeType(userDefinedType));
      }
      return typesType;
    } else {
      return null;
    }
  }

  private TypeType jaxbTypeType(ComposedTypeStructure userDefinedType) throws ModuleException {
    TypeType typeType = new TypeType();

    // TODO: support type hierarchy

    // TODO: support other kinds of UDT
    typeType.setCategory(CategoryType.UDT);

    // TODO: this is the common case; support other options
    typeType.setInstantiable(true);
    typeType.setFinal(false);

    if (StringUtils.isNotBlank(userDefinedType.getOriginalTypeName())) {
      typeType.setName(userDefinedType.getOriginalTypeName());
    } else {
      throw new ModuleException().withMessage("Error while exporting UDT structure: type name cannot be null");
    }

    typeType.setAttributes(jaxbAttributesType(userDefinedType.getDirectDescendantSubTypes()));

    return typeType;
  }

  private AttributesType jaxbAttributesType(Map<String, Type> directDescendantSubTypes) throws ModuleException {
    if (directDescendantSubTypes != null && !directDescendantSubTypes.isEmpty()) {
      AttributesType attributesType = new AttributesType();
      for (Map.Entry<String, Type> nameAndType : directDescendantSubTypes.entrySet()) {
        attributesType.getAttribute().add(jaxbAttributeType(nameAndType.getKey(), nameAndType.getValue()));
      }
      return attributesType;
    } else {
      return null;
    }
  }

  private AttributeType jaxbAttributeType(String name, Type type) throws ModuleException {
    AttributeType attributeType = new AttributeType();

    if (StringUtils.isNotBlank(name)) {
      attributeType.setName(name);
    } else {
      throw new ModuleException().withMessage("Error while exporting attribute structure: type name cannot be null");
    }

    if (type != null) {
      if (type instanceof ComposedTypeStructure) {
        LOGGER.debug("Saving UDT type '" + type.getOriginalTypeName() + "'(internal_id:" + type.hashCode() + ")");
        attributeType.setTypeName(type.getOriginalTypeName());
      } else {
        LOGGER.debug("Saving type '" + type.getOriginalTypeName() + "'(internal_id:" + type.hashCode() + ") as "
          + type.getSql2008TypeName());
        attributeType.setType(type.getSql2008TypeName());
        attributeType.setTypeOriginal(type.getOriginalTypeName());
      }
    } else {
      throw new ModuleException().withMessage("Error while exporting table structure: column type cannot be null");
    }

    // TODO: default value for type
    // if (StringUtils.isNotBlank(type.get)) {
    // columnType.setDefaultValue(type.getDefaultValue());
    // }

    // TODO: somehow set fields related to lob and complex types

    return attributeType;
  }

  private RoutinesType jaxbRoutinesType(String schemaName, List<RoutineStructure> routines) throws ModuleException {
    if (routines != null && !routines.isEmpty()) {
      RoutinesType routinesType = new RoutinesType();
      for (RoutineStructure routineStructure : routines) {
        routinesType.getRoutine().add(jaxbRoutineType(schemaName, routineStructure));
      }
      return routinesType;
    } else {
      return null;
    }
  }

  private RoutineType jaxbRoutineType(String schemaName, RoutineStructure routine) throws ModuleException {
    RoutineType routineType = new RoutineType();

    if (StringUtils.isNotBlank(routine.getName())) {
      routineType.setName(XMLUtils.encode(routine.getName()));
      routineType.setSpecificName((XMLUtils.encode(schemaName + "." + routine.getName())));
    } else {
      throw new ModuleException().withMessage("Error while exporting routine: routine name cannot be blank");
    }

    if (StringUtils.isNotBlank(routine.getDescription())) {
      routineType.setDescription(XMLUtils.encode(routine.getDescription()));
    }

    if (StringUtils.isNotBlank(routine.getSource())) {
      routineType.setSource(routine.getSource());
    }

    if (StringUtils.isNotBlank(routine.getBody())) {
      routineType.setBody(XMLUtils.encode(routine.getBody()));
    }

    if (StringUtils.isNotBlank(routine.getCharacteristic())) {
      routineType.setCharacteristic(routine.getCharacteristic());
    }

    if (StringUtils.isNotBlank(routine.getReturnType())) {
      routineType.setReturnType(routine.getReturnType());
    }

    routineType.setParameters(jaxbParametersType(routine.getParameters()));

    return routineType;
  }

  private ParametersType jaxbParametersType(List<Parameter> parameters) throws ModuleException {
    if (parameters != null && !parameters.isEmpty()) {
      ParametersType parametersType = new ParametersType();
      for (Parameter parameter : parameters) {
        parametersType.getParameter().add(jaxbParameterType(parameter));
      }
      return parametersType;
    } else {
      return null;
    }
  }

  private ParameterType jaxbParameterType(Parameter parameter) throws ModuleException {
    ParameterType parameterType = new ParameterType();

    if (StringUtils.isNotBlank(parameter.getName())) {
      parameterType.setName(parameter.getName());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting routine parameters: parameter name cannot be blank");
    }

    if (StringUtils.isNotBlank(parameter.getMode())) {
      parameterType.setMode(parameter.getMode());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting routine parameters: parameter mode cannot be blank");
    }

    if (parameter.getType() != null) {
      parameterType.setType(parameter.getType().getSql2008TypeName());
      parameterType.setTypeOriginal(parameter.getType().getOriginalTypeName());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting routine parameters: parameter type cannot be null");
    }

    if (StringUtils.isNotBlank(parameter.getDescription())) {
      parameterType.setDescription(XMLUtils.encode(parameter.getDescription()));
    }

    // todo: set these for complex types
    // parameterType.setTypeSchema(null);
    // parameterType.setTypeName(null);
    // parameterType.setParameterFields(null);

    return parameterType;
  }

  private ViewsType jaxbViewsType(List<ViewStructure> views) throws ModuleException {
    if (views != null && !views.isEmpty()) {
      ViewsType viewsType = new ViewsType();
      for (ViewStructure viewStructure : views) {
        viewsType.getView().add(jaxbViewType(viewStructure));
      }
      return viewsType;
    } else {
      return null;
    }
  }

  private ViewType jaxbViewType(ViewStructure view) throws ModuleException {
    ViewType viewType = new ViewType();

    if (StringUtils.isNotBlank(view.getName())) {
      viewType.setName(XMLUtils.encode(view.getName()));
    } else {
      throw new ModuleException().withMessage("Error while exporting view: view name cannot be null");
    }

    if (StringUtils.isNotBlank(view.getQuery())) {
      viewType.setQuery(XMLUtils.encode(view.getQuery()));
    }

    if (StringUtils.isNotBlank(view.getQueryOriginal())) {
      viewType.setQueryOriginal(XMLUtils.encode(view.getQueryOriginal()));
    }

    if (StringUtils.isNotBlank(view.getDescription())) {
      viewType.setDescription(XMLUtils.encode(view.getDescription()));
    }

    viewType.setColumns(jaxbColumnsType(view.getColumns()));

    return viewType;
  }

  private ColumnsType jaxbColumnsType(List<ColumnStructure> columns) throws ModuleException {
    if (columns != null && !columns.isEmpty()) {
      ColumnsType columnsType = new ColumnsType();
      for (int index = 0; index < columns.size(); index++) {
        ColumnStructure columnStructure = columns.get(index); // 0-based index
        columnsType.getColumn().add(jaxbColumnType(columnStructure, index + 1)); // 1-based
                                                                                 // index
      }
      return columnsType;
    } else {
      return null;
    }
  }

  private ColumnsType jaxbColumnsType(List<ColumnStructure> columns, int schemaIndex, int tableIndex)
    throws ModuleException {
    if (columns != null && !columns.isEmpty()) {
      ColumnsType columnsType = new ColumnsType();
      for (int index = 0; index < columns.size(); index++) {
        ColumnStructure columnStructure = columns.get(index); // 0-based index
        columnsType.getColumn().add(jaxbColumnType(columnStructure, schemaIndex, tableIndex, index + 1)); // 1-based
        // index
      }
      return columnsType;
    } else {
      return null;
    }
  }

  private ColumnType jaxbColumnType(ColumnStructure column, int columnIndex) throws ModuleException {
    ColumnType columnType = new ColumnType();

    if (StringUtils.isNotBlank(column.getName())) {
      columnType.setName(XMLUtils.encode(column.getName()));
    } else {
      throw new ModuleException().withMessage("Error while exporting table structure: column name cannot be null");
    }

    if (column.getType() != null) {
      if (column.getType() instanceof ComposedTypeStructure) {
        columnType.setTypeName(column.getType().getOriginalTypeName());
      } else {
        columnType.setType(column.getType().getSql2008TypeName());
        columnType.setTypeOriginal(column.getType().getOriginalTypeName());

        if (column.isNillable() != null) {
          columnType.setNullable(column.isNillable());
        } else {
          LOGGER.debug("column nullable property was null. changed it to false");
        }
      }
    } else {
      throw new ModuleException().withMessage("Error while exporting table structure: column type cannot be null");
    }

    if (StringUtils.isNotBlank(column.getDefaultValue())) {
      columnType.setDefaultValue(column.getDefaultValue());
    }

    if (StringUtils.isNotBlank(column.getDescription())) {
      columnType.setDescription(XMLUtils.encode(column.getDescription()));
    }

    // TODO: set fields related to lob and complex types

    // specific fields for lobs
    String xsdTypeFromColumnSql2008Type;
    try {
      xsdTypeFromColumnSql2008Type = Sql2008toXSDType.convert(column.getType(), reporter);
    } catch (UnknownTypeException e) {
      throw new ModuleException().withMessage("Could not get SQL2008 type").withCause(e);
    }

    if (column.getCardinality() != null) {
      columnType.setCardinality(column.getCardinality());
    }

    // columnType.setTypeSchema(null);
    // columnType.setTypeName(null);
    // columnType.setFields(null);
    // columnType.setMimeType(null);

    return columnType;
  }

  private ColumnType jaxbColumnType(ColumnStructure column, int schemaIndex, int tableIndex, int columnIndex)
    throws ModuleException {
    ColumnType columnType = new ColumnType();

    if (StringUtils.isNotBlank(column.getName())) {
      columnType.setName(XMLUtils.encode(column.getName()));
    } else {
      throw new ModuleException().withMessage("Error while exporting table structure: column name cannot be null");
    }

    if (column.getType() != null) {
      if (column.getType() instanceof ComposedTypeStructure) {
        columnType.setTypeName(column.getType().getOriginalTypeName());
      } else {
        columnType.setType(column.getType().getSql2008TypeName());
        columnType.setTypeOriginal(column.getType().getOriginalTypeName());

        if (column.isNillable() != null) {
          columnType.setNullable(column.isNillable());
        } else {
          LOGGER.debug("column nullable property was null. changed it to false");
        }
      }
    } else {
      throw new ModuleException().withMessage("Error while exporting table structure: column type cannot be null");
    }

    if (StringUtils.isNotBlank(column.getDefaultValue())) {
      columnType.setDefaultValue(column.getDefaultValue());
    }

    if (StringUtils.isNotBlank(column.getDescription())) {
      columnType.setDescription(XMLUtils.encode(column.getDescription()));
    }

    // TODO: set fields related to lob and complex types

    // specific fields for lobs
    String xsdTypeFromColumnSql2008Type;
    try {
      xsdTypeFromColumnSql2008Type = Sql2008toXSDType.convert(column.getType(), reporter);
    } catch (UnknownTypeException e) {
      throw new ModuleException().withMessage("Could not get SQL2008 type").withCause(e);
    }

    // don't set Folder if LOBs are being saved externally
    if ("clobType".equals(xsdTypeFromColumnSql2008Type) || "blobType".equals(xsdTypeFromColumnSql2008Type)) {
      if (!savingLobsExternally) {
        columnType
          .setLobFolder(contentPathStrategy.getRelativeInternalLobDirPath(schemaIndex, tableIndex, columnIndex));
      } else {
        columnType.setLobFolder("s" + schemaIndex + "_t" + tableIndex + "_c" + columnIndex);
      }
    }

    if (column.getCardinality() != null) {
      columnType.setCardinality(column.getCardinality());
    }

    return columnType;
  }

  private TablesType jaxbTablesType(SchemaStructure schema, List<TableStructure> tables) throws ModuleException {
    TablesType tablesType = new TablesType();
    if (tables != null && !tables.isEmpty()) {
      for (TableStructure tableStructure : tables) {
        tablesType.getTable().add(jaxbTableType(schema, tableStructure));
      }
    } else {
      LOGGER.info("Schema '{}' does not have any tables.", schema.getName());
    }
    return tablesType;
  }

  private TableType jaxbTableType(SchemaStructure schema, TableStructure table) throws ModuleException {
    TableType tableType = new TableType();

    if (StringUtils.isNotBlank(table.getName())) {
      tableType.setName(table.getName());
      tableType.setFolder(contentPathStrategy.getTableFolderName(table.getIndex()));
    } else {
      throw new ModuleException().withMessage("Error while exporting table structure: table name cannot be blank");
    }

    if (StringUtils.isNotBlank(table.getDescription())) {
      tableType.setDescription(XMLUtils.encode(table.getDescription()));
    }

    tableType.setColumns(jaxbColumnsType(table.getColumns(), schema.getIndex(), table.getIndex()));

    tableType.setPrimaryKey(jaxbPrimaryKeyType(table.getPrimaryKey()));

    tableType.setForeignKeys(jaxbForeignKeysType(table.getForeignKeys()));

    tableType.setCandidateKeys(jaxbCandidateKeysType(table.getCandidateKeys()));

    tableType.setCheckConstraints(jaxbCheckConstraintsType(table.getCheckConstraints()));

    tableType.setTriggers(jaxbTriggersType(table.getTriggers()));

    if (table.getRows() >= 0) {
      tableType.setRows(BigInteger.valueOf(table.getRows()));
    } else {
      throw new ModuleException().withMessage(
        "Error while exporting table structure: number of table rows was not set (or was set to negative value)");
    }

    return tableType;
  }

  private UniqueKeyType jaxbPrimaryKeyType(PrimaryKey primaryKey) throws ModuleException {
    if (primaryKey != null) {
      UniqueKeyType primaryKeyType = new UniqueKeyType();
      if (StringUtils.isNotBlank(primaryKey.getName())) {
        primaryKeyType.setName(primaryKey.getName());
      } else {
        throw new ModuleException().withMessage("Error while exporting primary key: name cannot be blank");
      }

      if (StringUtils.isNotBlank(primaryKey.getDescription())) {
        primaryKeyType.setDescription(XMLUtils.encode(primaryKey.getDescription()));
      }

      if (primaryKey.getColumnNames() != null && primaryKey.getColumnNames().size() > 0) {
        primaryKeyType.getColumn().addAll(primaryKey.getColumnNames());
      } else {
        // throw new
        // ModuleException("Error while exporting primary key: column list cannot be
        // empty");
        LOGGER.warn("Error while exporting primary key: column list cannot be empty");
      }

      return primaryKeyType;
    } else {
      return null;
    }
  }

  private TriggersType jaxbTriggersType(List<Trigger> triggers) throws ModuleException {
    if (triggers != null && !triggers.isEmpty()) {
      TriggersType triggersType = new TriggersType();
      for (Trigger trigger : triggers) {
        try {
          ActionTimeType.fromValue(trigger.getActionTime());
          triggersType.getTrigger().add(jaxbTriggerType(trigger));
        } catch (IllegalArgumentException e) {
          // report the migration data loss
          reporter.triggerDataLoss(this.getClass().getName(), trigger.getName());
          LOGGER.warn("Trigger {} ignored see migration report for more information", trigger.getName());
        }
      }

      if (triggersType.getTrigger().isEmpty()) {
        return null;
      }
      return triggersType;
    } else {
      return null;
    }
  }

  private TriggerType jaxbTriggerType(Trigger trigger) throws ModuleException {
    TriggerType triggerType = new TriggerType();

    if (StringUtils.isNotBlank(trigger.getName())) {
      triggerType.setName(XMLUtils.encode(trigger.getName()));
    } else {
      throw new ModuleException().withMessage("Error while exporting trigger: trigger name key name cannot be blank");
    }

    try {
      triggerType.setActionTime(ActionTimeType.fromValue(trigger.getActionTime()));
    } catch (IllegalArgumentException e) {
      throw new ModuleException().withMessage("Error while exporting trigger: trigger actionTime is invalid")
        .withCause(e);
    } catch (NullPointerException e) {
      throw new ModuleException().withMessage("Error while exporting trigger: trigger actionTime cannot be null")
        .withCause(e);
    }

    if (StringUtils.isNotBlank(trigger.getTriggerEvent())) {
      triggerType.setTriggerEvent(XMLUtils.encode(trigger.getTriggerEvent()));
    } else {
      throw new ModuleException().withMessage("Error while exporting trigger: trigger triggerEvent cannot be blank");
    }

    if (StringUtils.isNotBlank(trigger.getAliasList())) {
      triggerType.setAliasList(XMLUtils.encode(trigger.getAliasList()));
    }

    if (StringUtils.isNotBlank(trigger.getTriggeredAction())) {
      triggerType.setTriggeredAction(XMLUtils.encode(trigger.getTriggeredAction()));
    } else {
      throw new ModuleException().withMessage("Error while exporting trigger: trigger triggeredAction cannot be blank");
    }

    if (StringUtils.isNotBlank(trigger.getDescription())) {
      triggerType.setDescription(XMLUtils.encode(trigger.getDescription()));
    }

    return triggerType;
  }

  private CheckConstraintsType jaxbCheckConstraintsType(List<CheckConstraint> checkConstraints) throws ModuleException {
    if (checkConstraints != null && !checkConstraints.isEmpty()) {
      CheckConstraintsType checkConstraintsType = new CheckConstraintsType();
      for (CheckConstraint checkConstraint : checkConstraints) {
        checkConstraintsType.getCheckConstraint().add(jaxbCheckConstraintType(checkConstraint));
      }
      return checkConstraintsType;
    } else {
      return null;
    }
  }

  private CheckConstraintType jaxbCheckConstraintType(CheckConstraint checkConstraint) throws ModuleException {
    CheckConstraintType checkConstraintType = new CheckConstraintType();

    if (StringUtils.isNotBlank(checkConstraint.getName())) {
      checkConstraintType.setName(checkConstraint.getName());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting check constraint: check constraint key name cannot be null");
    }

    if (StringUtils.isNotBlank(checkConstraint.getCondition())) {
      checkConstraintType.setCondition(checkConstraint.getCondition());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting candidate key: check constraint condition cannot be null");
    }

    if (StringUtils.isNotBlank(checkConstraint.getDescription())) {
      checkConstraintType.setDescription(XMLUtils.encode(checkConstraint.getDescription()));
    }

    return checkConstraintType;
  }

  private CandidateKeysType jaxbCandidateKeysType(List<CandidateKey> candidateKeys) throws ModuleException {
    if (candidateKeys != null && !candidateKeys.isEmpty()) {
      CandidateKeysType candidateKeysType = new CandidateKeysType();
      for (CandidateKey candidateKey : candidateKeys) {
        candidateKeysType.getCandidateKey().add(jaxbCandidateKeyType(candidateKey));
      }
      return candidateKeysType;
    } else {
      return null;
    }
  }

  private UniqueKeyType jaxbCandidateKeyType(CandidateKey candidateKey) throws ModuleException {
    UniqueKeyType candidateKeyType = new UniqueKeyType();

    if (StringUtils.isNotBlank(candidateKey.getName())) {
      candidateKeyType.setName(candidateKey.getName());
    } else {
      throw new ModuleException().withMessage("Error while exporting candidate key: candidate key name cannot be null");
    }

    if (StringUtils.isNotBlank(candidateKey.getDescription())) {
      candidateKeyType.setDescription(XMLUtils.encode(candidateKey.getDescription()));
    }

    if (candidateKey.getColumns() != null && candidateKey.getColumns().size() > 0) {
      candidateKeyType.getColumn().addAll(candidateKey.getColumns());
    } else {
      throw new ModuleException()
        .withMessage("Error while exporting candidate key: columns cannot be be null or empty");
    }

    return candidateKeyType;
  }

  private ForeignKeysType jaxbForeignKeysType(List<ForeignKey> foreignKeys) throws ModuleException {
    if (foreignKeys != null && !foreignKeys.isEmpty()) {
      ForeignKeysType foreignKeysType = new ForeignKeysType();
      for (ForeignKey foreignKey : foreignKeys) {
        foreignKeysType.getForeignKey().add(jaxbForeignKeyType(foreignKey));
      }
      return foreignKeysType;
    } else {
      return null;
    }
  }

  private ForeignKeyType jaxbForeignKeyType(ForeignKey foreignKey) throws ModuleException {
    ForeignKeyType foreignKeyType = new ForeignKeyType();

    if (StringUtils.isNotBlank(foreignKey.getName())) {
      foreignKeyType.setName(foreignKey.getName());
    } else {
      throw new ModuleException().withMessage("Error while exporting foreign key: name cannot be blank");
    }

    if (StringUtils.isNotBlank(foreignKey.getReferencedSchema())) {
      foreignKeyType.setReferencedSchema(foreignKey.getReferencedSchema());
    } else {
      throw new ModuleException().withMessage("Error while exporting foreign key: referencedSchema cannot be blank");
    }

    if (StringUtils.isNotBlank(foreignKey.getReferencedTable())) {
      foreignKeyType.setReferencedTable(foreignKey.getReferencedTable());
    } else {
      throw new ModuleException().withMessage("Error while exporting foreign key: referencedTable cannot be blank");
    }

    if (foreignKey.getReferences() != null && foreignKey.getReferences().size() > 0) {
      for (Reference reference : foreignKey.getReferences()) {
        foreignKeyType.getReference().add(jaxbReferenceType(reference));
      }
    } else {
      throw new ModuleException().withMessage("Error while exporting foreign key: reference cannot be null or empty");
    }

    if (StringUtils.isNotBlank(foreignKey.getMatchType())) {
      foreignKeyType.setMatchType(MatchTypeType.fromValue(foreignKey.getMatchType()));
    }

    if (jaxbReferentialActionType(foreignKey.getDeleteAction()) != null) {
      foreignKeyType.setDeleteAction(jaxbReferentialActionType(foreignKey.getDeleteAction()));
    }

    if (jaxbReferentialActionType(foreignKey.getUpdateAction()) != null) {
      foreignKeyType.setUpdateAction(jaxbReferentialActionType(foreignKey.getUpdateAction()));
    }

    if (StringUtils.isNotBlank(foreignKey.getDescription())) {
      foreignKeyType.setDescription(XMLUtils.encode(foreignKey.getDescription()));
    }

    return foreignKeyType;
  }

  private ReferenceType jaxbReferenceType(Reference reference) {
    ReferenceType referenceType = new ReferenceType();

    if (StringUtils.isNotBlank(reference.getColumn())) {
      referenceType.setColumn(reference.getColumn());
    }

    if (StringUtils.isNotBlank(reference.getReferenced())) {
      referenceType.setReferenced(reference.getReferenced());
    }

    return referenceType;
  }

  private ReferentialActionType jaxbReferentialActionType(String referentialAction) {
    for (ReferentialActionType enumValue : ReferentialActionType.values()) {
      if (enumValue.value().equalsIgnoreCase(referentialAction)) {
        return enumValue;
      }
    }

    return null;
  }
}
