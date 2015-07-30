package com.databasepreservation.modules.siard.metadata;

import java.math.BigInteger;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.validation.Schema;

import com.databasepreservation.model.structure.*;
import com.databasepreservation.modules.siard.metadata.jaxb.siard1.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.SIARDHelper;
import com.databasepreservation.modules.siard.out.SIARDExportHelper;
import com.databasepreservation.modules.siard.path.PathStrategy;
import com.databasepreservation.modules.siard.write.WriteStrategy;

public class MetadataStrategySIARD1 implements MetadataStrategy {
	private final Logger logger = Logger.getLogger(MetadataStrategySIARD1.class);
	private static final String ENCODING = "UTF-8";

	private DatabaseStructure currentDatabaseStructure = null;
	private PathStrategy currentPathStrategy = null;
	private WriteStrategy currentWriteStrategy = null;

	//don't access this property directly, use getCurrentSIARDExportHelper() instead
	private SIARDExportHelper siardExportHelper = null;

	@Override
	public void output(DatabaseStructure database, PathStrategy paths, WriteStrategy writer)
			throws ModuleException {

		JAXBContext context;
		try {
			context = JAXBContext.newInstance("com.databasepreservation.modules.siard.metadata.jaxb.siard1");
		} catch (JAXBException e) {
			throw new ModuleException("Error loading JAXBContext", e);
		}

		currentDatabaseStructure = database;
		currentPathStrategy = paths;
		currentWriteStrategy = writer;

		SiardArchive xmlroot = jaxbSiardArchive(database);

		Marshaller m;
		try {
			m = context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		    m.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
	        m.marshal( xmlroot, System.out );
		} catch (JAXBException e) {
			throw new ModuleException("Error while Marshalling JAXB", e);
		}
	}

	private String getSql99Type(Type type) throws ModuleException{
		if( siardExportHelper == null ){
			siardExportHelper = SIARDExportHelper.getSIARDExportHelper(currentDatabaseStructure.getProductName());
		}

		try {
			return siardExportHelper.exportType(type);
		} catch (UnknownTypeException e) {
			throw new ModuleException(e);
		}
	}

	private SiardArchive jaxbSiardArchive(DatabaseStructure db) throws ModuleException{
		SiardArchive elem = new SiardArchive();
		elem.setArchivalDate(db.getArchivalDate());

		if(StringUtils.isNotBlank(db.getName())){
			elem.setDbname(db.getName());
		}else{
			throw new ModuleException("Error while exporting structure: dbname cannot be blank");
		}

		if(StringUtils.isNotBlank(db.getDescription())){
			elem.setDescription(db.getDescription());
		}

		if(StringUtils.isNotBlank(db.getArchiver())){
			elem.setArchiver(db.getArchiver());
		}

		if(StringUtils.isNotBlank(db.getArchiverContact())){
			elem.setArchiverContact(db.getArchiverContact());
		}

		if(StringUtils.isNotBlank(db.getDataOwner())){
			elem.setDataOwner(db.getDataOwner());
		}else{
			throw new ModuleException("Error while exporting structure: data owner cannot be blank");
		}

		if(StringUtils.isNotBlank(db.getDataOriginTimespan())){
			elem.setDataOriginTimespan(db.getDataOriginTimespan());
		}

		if(StringUtils.isNotBlank(db.getProducerApplication())){
			elem.setProducerApplication(db.getProducerApplication());
		}

		if(db.getArchivalDate() != null){
			elem.setArchivalDate(db.getArchivalDate());
		}

		//TODO: use some kind of message digest
		elem.setMessageDigest("");

		if(StringUtils.isNotBlank(db.getProductName())){
			if(StringUtils.isBlank(db.getProductVersion())){
				elem.setDatabaseProduct(db.getProductName());
			}else{
				elem.setDatabaseProduct(db.getProductName() + " " + db.getProductVersion());
			}
		}

		if(StringUtils.isNotBlank(db.getUrl())){
			elem.setConnection(db.getUrl());;
		}

		if(StringUtils.isNotBlank(db.getDatabaseUser())){
			elem.setDatabaseUser(db.getDatabaseUser());
		}

		if(StringUtils.isNotBlank(db.getClientMachine())){
			elem.setClientMachine(db.getClientMachine());
		}

		elem.setSchemas(jabxSchemasType(db.getSchemas()));
		elem.setUsers(jaxbUsersType(db.getUsers()));
		elem.setRoles(jaxbRolesType(db.getRoles()));
		elem.setPrivileges(jaxbPrivilegesType(db.getPrivileges()));

		return elem;
	}

	private PrivilegesType jaxbPrivilegesType(List<PrivilegeStructure> privileges) throws ModuleException {
		PrivilegesType privilegesType = new PrivilegesType();
		if (privileges != null) {
			for (PrivilegeStructure privilege : privileges) {
				privilegesType.getPrivilege().add(jaxbPrivilegeType(privilege));
			}
		}
		return privilegesType;
	}

	private PrivilegeType jaxbPrivilegeType(PrivilegeStructure privilege) throws ModuleException {
		PrivilegeType privilegeType = new PrivilegeType();

		if(StringUtils.isNotEmpty(privilege.getType())){
			privilegeType.setType(privilege.getType());
		} else {
			throw new ModuleException("Error while exporting users structure: privilege type cannot be blank");
		}

		if(StringUtils.isNotEmpty(privilege.getObject())){
			privilegeType.setObject(privilege.getObject());
		}else{
			privilegeType.setObject("unknown object");
			// logger.warn("Could not export privilege object");
			// TODO: check in which circumstances this happens
			throw new ModuleException("Error while exporting users structure: privilege object cannot be blank");
		}

		if(StringUtils.isNotBlank(privilege.getGrantor())){
			privilegeType.setGrantor(privilege.getGrantor());
		}else{
			throw new ModuleException("Error while exporting users structure: privilege grantor cannot be blank");
		}

		if(StringUtils.isNotBlank(privilege.getGrantee())){
			privilegeType.setGrantee(privilege.getGrantee());
		}else{
			throw new ModuleException("Error while exporting users structure: privilege grantee cannot be blank");
		}

		if(StringUtils.isNotBlank(privilege.getOption()) && SIARDHelper.isValidOption(privilege.getOption()) ){
			privilegeType.setOption(PrivOptionType.fromValue(privilege.getOption()));
		}

		if(StringUtils.isNotBlank(privilege.getDescription())){
			privilegeType.setDescription(privilege.getDescription());
		}

		return privilegeType;
	}

	private RolesType jaxbRolesType(List<RoleStructure> roles) throws ModuleException {
		RolesType rolesType = new RolesType();

		if(roles != null){
			for (RoleStructure role : roles) {
				rolesType.getRole().add(jaxbRoleType(role));
			}
		}
		return rolesType;
	}

	private RoleType jaxbRoleType(RoleStructure role) throws ModuleException {
		RoleType roleType = new RoleType();

		if(StringUtils.isNotBlank(role.getName())){
			roleType.setName(role.getName());
		}else{
			throw new ModuleException("Error while exporting users structure: user name cannot be blank");
		}

		if(StringUtils.isNotEmpty(role.getAdmin())){
			roleType.setAdmin(role.getAdmin());
		}else{
			roleType.setAdmin("");
			// TODO: check in which circumstances this happens
			throw new ModuleException("Error while exporting users structure: role admin cannot be null");
		}

		if(StringUtils.isNotBlank(role.getDescription())){
			roleType.setDescription(role.getDescription());
		}

		return roleType;
	}

	private UsersType jaxbUsersType(List<UserStructure> users) throws ModuleException {
		UsersType usersType = new UsersType();

		if(users != null){
			for (UserStructure user : users) {
				usersType.getUser().add(jaxbUserType(user));
			}
		}

		return usersType;
	}

	private UserType jaxbUserType(UserStructure user) throws ModuleException {
		UserType userType = new UserType();

		if(StringUtils.isNotBlank(user.getName())){
			userType.setName(user.getName());
		}else{
			throw new ModuleException("Error while exporting users structure: user name cannot be blank");
		}

		if(StringUtils.isNotBlank(user.getDescription())){
			userType.setDescription(user.getDescription());
		}

		return userType;
	}

	private SchemasType jabxSchemasType(List<SchemaStructure> schemas) throws ModuleException {
		SchemasType schemasType = new SchemasType();
		if(schemas != null){
			for (SchemaStructure schema : schemas) {
				schemasType.getSchema().add(jaxbSchemaType(schema));
			}

		}
		return schemasType;
	}

	private SchemaType jaxbSchemaType(SchemaStructure schema) throws ModuleException {
		SchemaType schemaType = new SchemaType();

		if(StringUtils.isNotBlank(schema.getName())){
			schemaType.setName(schema.getName());
			schemaType.setFolder(currentPathStrategy.schemaFolder(schema.getIndex()));
		}else{
			throw new ModuleException("Error while exporting schema structure: schema name cannot be blank");
		}

		if(StringUtils.isNotBlank(schema.getDescription())){
			schemaType.setDescription(schema.getDescription());
		}

		schemaType.setTables(jaxbTablesType(schema, schema.getTables()));
		schemaType.setViews(jaxbViewsType(schema.getViews()));
		schemaType.setRoutines(jaxbRoutinesType(schema.getRoutines()));

		return schemaType;
	}

	private RoutinesType jaxbRoutinesType(List<RoutineStructure> routines) throws ModuleException {
		RoutinesType routinesType = new RoutinesType();

		if(routines != null){
			for (RoutineStructure routineStructure : routines) {
				routinesType.getRoutine().add(jaxbRoutineType(routineStructure));
			}
		}

		return routinesType;
	}

	private RoutineType jaxbRoutineType(RoutineStructure routine) throws ModuleException {
		RoutineType routineType = new RoutineType();

		if(StringUtils.isNotBlank(routine.getName())){
			routineType.setName(routine.getName());
		}else{
			throw new ModuleException("Error while exporting routine: routine name cannot be blank");
		}

		if(StringUtils.isNotBlank(routine.getDescription())){
			routineType.setDescription(routine.getDescription());
		}

		if(StringUtils.isNotBlank(routine.getSource())){
			routineType.setSource(routine.getSource());
		}

		if(StringUtils.isNotBlank(routine.getBody())){
			routineType.setBody(routine.getBody());
		}

		if(StringUtils.isNotBlank(routine.getCharacteristic())){
			routineType.setCharacteristic(routine.getCharacteristic());
		}

		if(StringUtils.isNotBlank(routine.getReturnType())){
			routineType.setReturnType(routine.getReturnType());
		}

		routineType.setParameters(jaxbParametersType(routine.getParameters()));

		return routineType;
	}

	private ParametersType jaxbParametersType(List<Parameter> parameters) throws ModuleException {
		ParametersType parametersType = new ParametersType();

		if(parameters != null){
			for (Parameter parameter : parameters) {
				parametersType.getParameter().add(jaxbParameterType(parameter));
			}
		}

		return parametersType;
	}

	private ParameterType jaxbParameterType(Parameter parameter) throws ModuleException {
		ParameterType parameterType = new ParameterType();

		if(StringUtils.isNotBlank(parameter.getName())){
			parameterType.setName(parameter.getName());
		}else{
			throw new ModuleException("Error while exporting routine parameters: parameter name cannot be blank");
		}

		if(StringUtils.isNotBlank(parameter.getMode())){
			parameterType.setMode(parameter.getMode());
		}else{
			throw new ModuleException("Error while exporting routine parameters: parameter mode cannot be blank");
		}

		if(parameter.getType() != null){
			parameterType.setType(getSql99Type(parameter.getType()));
			parameterType.setTypeOriginal(parameter.getType().getOriginalTypeName());
		}else{
			throw new ModuleException("Error while exporting routine parameters: parameter type cannot be null");
		}

		if(StringUtils.isNotBlank(parameter.getDescription())){
			parameterType.setDescription(parameter.getDescription());
		}

		return parameterType;
	}

	private ViewsType jaxbViewsType(List<ViewStructure> views) throws ModuleException {
		ViewsType viewsType = new ViewsType();

		if(views != null){
			for (ViewStructure viewStructure : views) {
				viewsType.getView().add(jaxbViewType(viewStructure));
			}
		}

		return viewsType;
	}

	private ViewType jaxbViewType(ViewStructure view) throws ModuleException {
		ViewType viewType = new ViewType();

		if(StringUtils.isNotBlank(view.getName())){
			viewType.setName(view.getName());
		}else{
			throw new ModuleException("Error while exporting view: view name cannot be null");
		}

		if(StringUtils.isNotBlank(view.getQuery())){
			viewType.setQuery(view.getQuery());
		}

		if(StringUtils.isNotBlank(view.getQueryOriginal())){
			viewType.setQueryOriginal(view.getQueryOriginal());
		}

		if(StringUtils.isNotBlank(view.getDescription())){
			viewType.setDescription(view.getDescription());
		}

		viewType.setColumns(jaxbColumnsType(view.getColumns()));

		return viewType;
	}

	private ColumnsType jaxbColumnsType(List<ColumnStructure> columns) throws ModuleException {
		ColumnsType columnsType = new ColumnsType();

		if(columns != null){
			for (ColumnStructure columnStructure : columns) {
				columnsType.getColumn().add(jaxbcolumnType(columnStructure));
			}
		}

		return columnsType;
	}

	private ColumnType jaxbcolumnType(ColumnStructure column) throws ModuleException {
		ColumnType columnType = new ColumnType();

		if(StringUtils.isNotBlank(column.getName())){
			columnType.setName(column.getName());
		}else{
			throw new ModuleException("Error while exporting table structure: column name cannot be null");
		}

		if(column.getType() != null){
			columnType.setType(getSql99Type(column.getType()));
		}else{
			throw new ModuleException("Error while exporting table structure: column type cannot be null");
		}

		if(StringUtils.isNotBlank(column.getDefaultValue())){
			columnType.setDefaultValue(column.getDefaultValue());
		}

		if(column.isNillable() != null){
			columnType.setNullable(column.getNillable());
		}else{
			logger.warn("column nullable property was null. changed it to false");
		}

		if(StringUtils.isNotBlank(column.getDescription())){
			columnType.setDescription(column.getDescription());
		}

		return columnType;
	}

	private TablesType jaxbTablesType(SchemaStructure schema, List<TableStructure> tables) throws ModuleException {
		TablesType tablesType = new TablesType();

		if(tables != null){
			for (TableStructure tableStructure : tables) {
				tablesType.getTable().add(jaxbTableType(schema, tableStructure));
			}
		}else{
			logger.info(String.format("Schema %s does not have any tables.", schema.getName()));
		}

		return tablesType;
	}

	private TableType jaxbTableType(SchemaStructure schema, TableStructure table) throws ModuleException {
		TableType tableType = new TableType();

		if(StringUtils.isNotBlank(table.getName())){
			tableType.setName(table.getName());
			tableType.setFolder(currentPathStrategy.tableFolder(schema.getIndex(), table.getIndex()));
		}else{
			throw new ModuleException("Error while exporting table structure: table name cannot be blank");
		}

		if(StringUtils.isNotBlank(table.getDescription())){
			tableType.setDescription(table.getDescription());
		}

		tableType.setColumns(jaxbColumnsType(table.getColumns()));

		tableType.setPrimaryKey(jaxbPrimaryKeyType(table.getPrimaryKey()));

		tableType.setForeignKeys(jaxbForeignKeysType(table.getForeignKeys()));

		tableType.setCandidateKeys(jaxbCandidateKeysType(table.getCandidateKeys()));

		tableType.setCheckConstraints(jaxbCheckConstraintsType(table.getCheckConstraints()));

		tableType.setTriggers(jaxbTriggersType(table.getTriggers()));

		if(table.getRows() >= 0){
			tableType.setRows(BigInteger.valueOf(table.getRows()));
		}else{
			throw new ModuleException("Error while exporting table structure: number of table rows was not set (or was set to negative value)");
		}

		return tableType;
	}

	private PrimaryKeyType jaxbPrimaryKeyType(PrimaryKey primaryKey) throws ModuleException {
		PrimaryKeyType primaryKeyType = new PrimaryKeyType();

		if (primaryKey != null) {
			if(StringUtils.isNotBlank(primaryKey.getName())){
				primaryKeyType.setName(primaryKey.getName());
			}else{
				throw new ModuleException("Error while exporting primary key: name cannot be blank");
			}

			if(StringUtils.isNotBlank(primaryKey.getDescription())){
				primaryKeyType.setDescription(primaryKey.getDescription());
			}

			if(primaryKey.getColumnNames() != null && primaryKey.getColumnNames().size() > 0){
				primaryKeyType.getColumn().addAll(primaryKey.getColumnNames());
			}else{
				//throw new ModuleException("Error while exporting primary key: column list cannot be empty");
				logger.warn("Error while exporting primary key: column list cannot be empty");
			}
		}

		return primaryKeyType;
	}

	private TriggersType jaxbTriggersType(List<Trigger> triggers) throws ModuleException {
		TriggersType triggersType = new TriggersType();

		if(triggers != null){
			for (Trigger trigger : triggers) {
				triggersType.getTrigger().add(jaxbTriggerType(trigger));
			}
		}

		return triggersType;
	}

	private TriggerType jaxbTriggerType(Trigger trigger) throws ModuleException {
		TriggerType triggerType = new TriggerType();

		if( StringUtils.isNotBlank(trigger.getName())){
			triggerType.setName(SIARDHelper.encode(trigger.getName()));
		}else{
			throw new ModuleException("Error while exporting trigger: trigger name key name cannot be blank");
		}

		try{
			triggerType.setActionTime(ActionTimeType.fromValue(trigger.getActionTime()));
		}catch(IllegalArgumentException e){
			throw new ModuleException("Error while exporting trigger: trigger actionTime is invalid", e);
		}catch(NullPointerException e){
			throw new ModuleException("Error while exporting trigger: trigger actionTime cannot be null", e);
		}

		if( StringUtils.isNotBlank(trigger.getTriggerEvent()) ){
			triggerType.setTriggerEvent(SIARDHelper.encode(trigger.getTriggerEvent()));
		}else{
			throw new ModuleException("Error while exporting trigger: trigger triggerEvent cannot be blank");
		}

		if(StringUtils.isNotBlank(trigger.getAliasList())){
			triggerType.setAliasList(trigger.getAliasList());
		}

		if( StringUtils.isNotBlank(trigger.getTriggeredAction()) ){
			triggerType.setTriggeredAction(SIARDHelper.encode(trigger.getTriggeredAction()));
		}else{
			throw new ModuleException("Error while exporting trigger: trigger triggeredAction cannot be black");
		}

		if(StringUtils.isNotBlank(trigger.getDescription())){
			triggerType.setDescription(trigger.getDescription());
		}

		return triggerType;
	}

	private CheckConstraintsType jaxbCheckConstraintsType(List<CheckConstraint> checkConstraints) throws ModuleException {
		CheckConstraintsType checkConstraintsType = new CheckConstraintsType();

		if(checkConstraints != null){
			for (CheckConstraint checkConstraint : checkConstraints) {
				checkConstraintsType.getCheckConstraint().add(jaxbCheckConstraintType(checkConstraint));
			}
		}

		return checkConstraintsType;
	}

	private CheckConstraintType jaxbCheckConstraintType(CheckConstraint checkConstraint) throws ModuleException {
		CheckConstraintType checkConstraintType = new CheckConstraintType();

		if(StringUtils.isNotBlank(checkConstraint.getName())){
			checkConstraintType.setName(checkConstraint.getName());
		}else{
			throw new ModuleException("Error while exporting check constraint: check constraint key name cannot be null");
		}

		if(StringUtils.isNotBlank(checkConstraint.getCondition())){
			checkConstraintType.setCondition(checkConstraint.getCondition());
		}else{
			throw new ModuleException("Error while exporting candidate key: check constraint condition cannot be null");
		}

		if(StringUtils.isNotBlank(checkConstraint.getDescription())){
			checkConstraintType.setDescription(checkConstraint.getDescription());
		}

		return checkConstraintType;
	}

	private CandidateKeysType jaxbCandidateKeysType(List<CandidateKey> candidateKeys) throws ModuleException {
		CandidateKeysType candidateKeysType = new CandidateKeysType();

		if(candidateKeys != null){
			for (CandidateKey candidateKey : candidateKeys) {
				candidateKeysType.getCandidateKey().add(jaxbCandidateKeyType(candidateKey));
			}
		}

		return candidateKeysType;
	}

	private CandidateKeyType jaxbCandidateKeyType(CandidateKey candidateKey) throws ModuleException {
		CandidateKeyType candidateKeyType = new CandidateKeyType();

		if(StringUtils.isNotBlank(candidateKey.getName())){
			candidateKeyType.setName(candidateKey.getName());
		}else{
			throw new ModuleException("Error while exporting candidate key: candidate key name cannot be null");
		}

		if(StringUtils.isNotBlank(candidateKey.getDescription())){
			candidateKeyType.setDescription(candidateKey.getDescription());
		}

		if(candidateKey.getColumns() != null && candidateKey.getColumns().size() > 0){
			candidateKeyType.getColumn().addAll(candidateKey.getColumns());
		} else {
			throw new ModuleException("Error while exporting candidate key: columns cannot be be null or empty");
		}

		return candidateKeyType;
	}

	private ForeignKeysType jaxbForeignKeysType(List<ForeignKey> foreignKeys) throws ModuleException {
		ForeignKeysType foreignKeysType = new ForeignKeysType();

		if(foreignKeys != null){
			for (ForeignKey foreignKey : foreignKeys) {
				foreignKeysType.getForeignKey().add(jaxbForeignKeyType(foreignKey));
			}
		}

		return foreignKeysType;
	}

	private ForeignKeyType jaxbForeignKeyType(ForeignKey foreignKey) throws ModuleException {
		ForeignKeyType foreignKeyType = new ForeignKeyType();

		if(StringUtils.isNotBlank(foreignKey.getName())){
			foreignKeyType.setName(foreignKey.getName());
		}else{
			throw new ModuleException("Error while exporting foreign key: name cannot be blank");
		}

		if(StringUtils.isNotBlank(foreignKey.getReferencedSchema())){
			foreignKeyType.setReferencedSchema(foreignKey.getReferencedSchema());
		}else{
			throw new ModuleException("Error while exporting foreign key: referencedSchema cannot be blank");
		}

		if(StringUtils.isNotBlank(foreignKey.getReferencedTable())){
			foreignKeyType.setReferencedTable(foreignKey.getReferencedTable());
		}else{
			throw new ModuleException("Error while exporting foreign key: referencedTable cannot be blank");
		}


		if (foreignKey.getReferences() != null && foreignKey.getReferences().size() > 0) {
			for (Reference reference : foreignKey.getReferences()) {
				foreignKeyType.getReference().add(jaxbReferencetype(reference));
			}
		} else {
			throw new ModuleException("Error while exporting foreign key: reference cannot be null or empty");
		}

		if (StringUtils.isNotBlank(foreignKey.getMatchType())) {
			foreignKeyType.setMatchType(MatchTypeType.fromValue(foreignKey.getMatchType()));
		}

		if (StringUtils.isNotBlank(foreignKey.getDeleteAction())) {
			foreignKeyType.setDeleteAction(foreignKey.getDeleteAction());
		}

		if (StringUtils.isNotBlank(foreignKey.getUpdateAction())) {
			foreignKeyType.setUpdateAction(foreignKey.getUpdateAction());
		}

		if (StringUtils.isNotBlank(foreignKey.getDescription())) {
			foreignKeyType.setDescription(foreignKey.getDescription());
		}

		return foreignKeyType;
	}

	private ReferenceType jaxbReferencetype(Reference reference) {
		ReferenceType referenceType = new ReferenceType();

		if(StringUtils.isNotBlank(reference.getColumn())){
			referenceType.setColumn(reference.getColumn());
		}

		if(StringUtils.isNotBlank(reference.getReferenced())){
			referenceType.setReferenced(reference.getReferenced());
		}

		return referenceType;
	}
}
