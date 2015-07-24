package com.databasepreservation.modules.siard.in;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
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
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.SIARDHelper;
import com.databasepreservation.utils.JodaUtils;

/**
 *
 * @author Miguel Coutada
 *
 */

public class SIARDImportModule implements DatabaseImportModule {

	public static final String SIARD_DEFAULT_FILE_NAME = "Default.siard";

	// private static final String SCHEMA_VERSION = "UNKNONW";

	private static final String ENCODING = "UTF-8";

	private final Logger logger = Logger.getLogger(SIARDImportModule.class);

	private SAXParser saxParser;

	private ZipFile zipFile;

	private InputStream header;

	private InputStream currentInputStream;

	private DatabaseStructure dbStructure;


	/**
	 * SIARD import module constructor using a package directory
	 *
	 * @param baseDir
	 * @throws ModuleException
	 */
	public SIARDImportModule(final File baseDir) throws ModuleException {
		this(baseDir, SIARD_DEFAULT_FILE_NAME);
	}

	public SIARDImportModule(final File siardPackage, String siardFileName)
			throws ModuleException {
		try {
			if (!siardPackage.exists()) {
				throw new ModuleException("Siard package could not be found");
			}
			zipFile = new ZipFile(siardPackage);
			this.header = null;
			this.currentInputStream = null;
			initSAXParserFactory();
		} catch (IOException e) {

		}
	}

	protected void initSAXParserFactory()
			throws ModuleException {
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		try {
			this.saxParser = saxParserFactory.newSAXParser();
		} catch (SAXException e) {
			throw new ModuleException("Error initializing SAX parser", e);
		} catch (ParserConfigurationException e) {
			throw new ModuleException("Error initializing SAX parser", e);
		}
	}

	protected void setHeader() throws ModuleException {
		try {
			ZipArchiveEntry metadata = zipFile.getEntry("header/metadata.xml");
			if (metadata == null) {
				throw new ModuleException("SIARD package is not well formed: "
						+ "header/metadata.xml missing");
			}
			this.header = zipFile.getInputStream(metadata);
		} catch (IOException e) {
			throw new ModuleException("An error ocurred while accessing to "
					+ "a file in SIARD package", e);
		}
	}

	protected void setCurrentInputStream(SchemaStructure schema, String schemaFolder,
			TableStructure table, String tableFolder) throws ModuleException {
		try {
			ZipArchiveEntry content = zipFile.getEntry("content/"
					+ schemaFolder + "/" + tableFolder
					+ "/" + tableFolder + ".xml");
			if (content == null) {
				throw new ModuleException("SIARD package is not well formed");
			}

			this.currentInputStream = zipFile.getInputStream(content);
		} catch (IOException e) {
			throw new ModuleException("An error ocurred while accessing to "
					+ "a file in SIARD package", e);
		}
	}

	protected boolean validateSchema() throws ModuleException {
	    try {
	    	SchemaFactory factory = SchemaFactory.newInstance(
	    			XMLConstants.W3C_XML_SCHEMA_NS_URI);

    		ZipArchiveEntry schemaEntry;
    		try {
    			schemaEntry = zipFile.getEntry("header/metadata.xsd");
    		} catch (Exception e) {
    			throw new ModuleException(
    					"header/metadata.xsd could not be found", e);
    		}

	        InputStream schemaIS = zipFile.getInputStream(schemaEntry);
	        Schema schema = factory.newSchema(new StreamSource(schemaIS));

	        Validator validator = schema.newValidator();

	        ZipArchiveEntry metadata;
	        try {
	        	metadata = zipFile.getEntry("header/metadata.xml");
	        } catch (Exception e) {
	        	throw new ModuleException(
    					"heder/metadata.xml could not be found", e);
	        }
	        InputStream metadataIS = zipFile.getInputStream(metadata);
	        Source metadataSource = new StreamSource(metadataIS);

	        validator.validate(metadataSource);
		} catch (IOException e) {
			logger.error("Error validating schema", e);
			return false;
		} catch (SAXException e) {
			logger.error("Error validating schema", e);
			return false;
		}
	    return true;
	}

	@Override
	public void getDatabase(DatabaseHandler handler)
			throws ModuleException, UnknownTypeException, InvalidDataException {
		SIARDHeaderSAXHandler siardHeaderSAXHandler =
				new SIARDHeaderSAXHandler(handler);
		SIARDContentSAXHandler siardContentSAXHandler =
				new SIARDContentSAXHandler(handler);

		handler.initDatabase();
		try {
			if (!validateSchema()) {
				throw new ModuleException("Schema is not valid!");
			}
			setHeader();
			saxParser.parse(header, siardHeaderSAXHandler);
			if (siardHeaderSAXHandler.getErrors().size() > 0) {
				throw new ModuleException(siardHeaderSAXHandler.getErrors());
			}
			header.close();

			dbStructure = siardHeaderSAXHandler.getDatabaseStructure();
			for (SchemaStructure schema : dbStructure.getSchemas()) {
				for (TableStructure table : schema.getTables()) {
					setCurrentInputStream(
							schema, siardHeaderSAXHandler.schemaFolders.get(schema.getName()),
							table, siardHeaderSAXHandler.tableFolders.get(table.getId()));
					// TODO siardContentSAXHandler.setCurrentSchema(schema)
					siardContentSAXHandler.setCurrentTable(table);
					saxParser.parse(currentInputStream, siardContentSAXHandler);
					if (siardContentSAXHandler.getErrors().size() > 0) {
						throw new ModuleException(
								siardHeaderSAXHandler.getErrors());
					}
					currentInputStream.close();
				}
			}
			zipFile.close();
			handler.finishDatabase();

		} catch (SAXException e) {
			throw new ModuleException(
					"An error occurred while importing SIARD", e);
		} catch (IOException e) {
			throw new ModuleException("Error reading SIARD", e);
		}
	}

	public class SIARDHeaderSAXHandler extends DefaultHandler {

		private DatabaseHandler handler;
		private Map<String, Throwable> errors;

		private final Stack<String> tagsStack = new Stack<String>();
		private final StringBuilder tempVal = new StringBuilder();

		private DatabaseStructure dbStructure;
		private List<SchemaStructure> schemas;
		private SchemaStructure schema;
		private List<TableStructure> tables;
		private TableStructure table;
		private List<ColumnStructure> columns;
		private ColumnStructure column;
		private Type type;
		private PrimaryKey primaryKey;
		private List<String> primaryKeyColumns;
		private List<ForeignKey> foreignKeys;
		private ForeignKey foreignKey;
		private List<Reference> references;
		private Reference reference;
		private List<CandidateKey> candidateKeys;
		private List<String> candidateKeyColumns;
		private CandidateKey candidateKey;
		private List<CheckConstraint> checkConstraints;
		private CheckConstraint checkConstraint;
		private List<Trigger> triggers;
		private Trigger trigger;
		private List<ViewStructure> views;
		private ViewStructure view;
		private List<RoutineStructure> routines;
		private RoutineStructure routine;
		private List<Parameter> parameters;
		private Parameter parameter;
		private List<UserStructure> users;
		private UserStructure user;
		private List<RoleStructure> roles;
		private RoleStructure role;
		private List<PrivilegeStructure> privileges;
		private PrivilegeStructure privilege;
		private String messageDigest;

		private Map<String,String> tableFolders = new HashMap<String,String>();
		private Map<String,String> schemaFolders = new HashMap<String,String>();

		private int schemaIndex = 1;
		private int tableIndex = 1;

		public SIARDHeaderSAXHandler(DatabaseHandler handler) {
			this.handler = handler;
			this.errors = new TreeMap<String, Throwable>();
		}

		public Map<String, Throwable> getErrors() {
			return errors;
		}

		/**
		 * Gets the table's folder name from the table's id
		 * @param tableId
		 * @return
		 */
		public String getTableFolder(String tableId){
			return tableFolders.get(tableId);
		}

		/**
		 * Gets the schema's folder name from the schema's name
		 * @param schemaName
		 * @return
		 */
		public String getSchemaFolder(String schemaName){
			return schemaFolders.get(schemaName);
		}

		@Override
		public void startDocument() {
			pushTag("");
		}

		@Override
		public void endDocument() throws SAXException {
//			logger.debug(dbStructure.toString());
			try {
				handler.handleStructure(dbStructure);
			} catch (ModuleException e) {
				logger.error("An error occurred "
						+ "while handling Database Structure", e);
				throw new SAXException();
			} catch (UnknownTypeException e) {
				logger.error("An error occurred "
						+ "while handling Database Structure", e);
			}
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attr) {
			pushTag(qName);
			tempVal.setLength(0);

			if (qName.equalsIgnoreCase("siardArchive")) {
				dbStructure = new DatabaseStructure();
				double version = Double.parseDouble(attr.getValue("version"));
				if (version > 1.0) {
					errors.put("SIARD version is not 1.0. "
							+ "Currently only version 1.0 is supported", null);
				}
			} else if (qName.equalsIgnoreCase("schemas")) {
				schemas = new ArrayList<SchemaStructure>();
			} else if (qName.equalsIgnoreCase("schema")) {
				schema = new SchemaStructure();
				schema.setIndex(schemaIndex);
				schemaIndex++;
			} else if (qName.equalsIgnoreCase("tables")) {
				tables = new ArrayList<TableStructure>();
			} else if (qName.equalsIgnoreCase("table")) {
				table = new TableStructure();
				table.setIndex(tableIndex);
				tableIndex++;
			} else if (qName.equalsIgnoreCase("columns")) {
				columns = new ArrayList<ColumnStructure>();
			} else if (qName.equalsIgnoreCase("column")) {
				column = new ColumnStructure();
			} else if (qName.equalsIgnoreCase("primaryKey")) {
				primaryKey = new PrimaryKey();
				primaryKeyColumns = new ArrayList<String>();
			} else if (qName.equalsIgnoreCase("foreignKeys")) {
				foreignKeys = new ArrayList<ForeignKey>();
			} else if (qName.equalsIgnoreCase("foreignKey")) {
				foreignKey = new ForeignKey();
				references = new ArrayList<Reference>();
			} else if (qName.equalsIgnoreCase("reference")) {
				reference = new Reference();
			} else if (qName.equalsIgnoreCase("candidateKeys")) {
				candidateKeys = new ArrayList<CandidateKey>();
			} else if (qName.equalsIgnoreCase("candidateKey")) {
				candidateKey = new CandidateKey();
				candidateKeyColumns = new ArrayList<String>();
			} else if (qName.equalsIgnoreCase("checkConstraints")) {
				checkConstraints = new ArrayList<CheckConstraint>();
			} else if (qName.equalsIgnoreCase("checkConstraint")) {
				checkConstraint = new CheckConstraint();
			} else if (qName.equalsIgnoreCase("triggers")) {
				triggers = new ArrayList<Trigger>();
			} else if (qName.equalsIgnoreCase("trigger")) {
				trigger = new Trigger();
			} else if (qName.equalsIgnoreCase("views")) {
				views = new ArrayList<ViewStructure>();
			} else if (qName.equalsIgnoreCase("view")) {
				view = new ViewStructure();
			} else if (qName.equalsIgnoreCase("routines")) {
				routines = new ArrayList<RoutineStructure>();
			} else if (qName.equalsIgnoreCase("routine")) {
				routine = new RoutineStructure();
			} else if (qName.equals("parameters")) {
				parameters = new ArrayList<Parameter>();
			} else if (qName.equalsIgnoreCase("parameter")) {
				parameter = new Parameter();
			} else if (qName.equalsIgnoreCase("users")) {
				users = new ArrayList<UserStructure>();
			} else if (qName.equalsIgnoreCase("user")) {
				user = new UserStructure();
			} else if (qName.equalsIgnoreCase("roles")) {
				roles = new ArrayList<RoleStructure>();
			} else if (qName.equalsIgnoreCase("role")) {
				role = new RoleStructure();
			} else if (qName.equalsIgnoreCase("privileges")) {
				privileges = new ArrayList<PrivilegeStructure>();
			} else if (qName.equalsIgnoreCase("privilege")) {
				privilege = new PrivilegeStructure();
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			String tag = peekTag();
			if (!qName.equals(tag)) {
				throw new InternalError();
			}

			popTag();
			String parentTag = peekTag();
			String trimmedVal = tempVal.toString().trim();

			if (tag.equalsIgnoreCase("dbname")) {
				dbStructure.setName(trimmedVal);
			} else if (tag.equalsIgnoreCase("archiver")) {
				dbStructure.setArchiver(trimmedVal);
			} else if (tag.equalsIgnoreCase("archiverContact")) {
				dbStructure.setArchiverContact(trimmedVal);
			} else if (tag.equalsIgnoreCase("dataOwner")) {
				dbStructure.setDataOwner(trimmedVal);
			} else if (tag.equalsIgnoreCase("dataOriginTimespan")) {
				dbStructure.setDataOriginTimespan(trimmedVal);
			} else if (tag.equalsIgnoreCase("producerApplication")) {
				dbStructure.setProducerApplication(trimmedVal);
			} else if (tag.equalsIgnoreCase("archivalDate")) {
				dbStructure.setArchivalDate(JodaUtils.xs_date_parse(trimmedVal));
			} else if (tag.equalsIgnoreCase("messageDigest")) {
				messageDigest = trimmedVal;
			} else if (tag.equalsIgnoreCase("clientMachine")) {
				dbStructure.setClientMachine(trimmedVal);
			} else if (tag.equalsIgnoreCase("databaseProduct")) {
				dbStructure.setProductName(trimmedVal);
			} else if (tag.equalsIgnoreCase("connection")) {
				dbStructure.setUrl(trimmedVal);
			} else if (tag.equalsIgnoreCase("databaseUser")) {
				dbStructure.setDatabaseUser(trimmedVal);
			} else if (tag.equalsIgnoreCase("name")) {
				if (parentTag.equalsIgnoreCase("table")) {
					if (schema.getName() != null) {
						table.setId(schema.getName() + "." + trimmedVal);
					} else {
						logger.error("Error while getting schema name: "
								+ "schema name not defined.");
					}
					table.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("schema")) {
					schema.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("column")) {
					column.setName(trimmedVal);
					if (table.getName() != null) {
						column.setId(table.getId() + "." + trimmedVal);
					}
				} else if (parentTag.equalsIgnoreCase("primaryKey")) {
					primaryKey.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("foreignKey")) {
					foreignKey.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("candidateKey")) {
					candidateKey.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("checkConstraint")) {
					checkConstraint.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("trigger")) {
					trigger.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("view")) {
					view.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("routine")) {
					routine.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("user")) {
					user.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("parameter")) {
					parameter.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("role")) {
					role.setName(trimmedVal);
				}
			} else if (tag.equalsIgnoreCase("folder")) {
				if (parentTag.equalsIgnoreCase("table")) {
					tableFolders.put(table.getId(), trimmedVal);
				} else if (parentTag.equalsIgnoreCase("schema")) {
					schemaFolders.put(schema.getName(), trimmedVal);
				} else if (parentTag.equalsIgnoreCase("column")) {
					//columnFolders.put(column.getId(), trimmedVal);
				}
			} else if (tag.equalsIgnoreCase("description")) {
				if (parentTag.equalsIgnoreCase("siardArchive")) {
					dbStructure.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("table")) {
					table.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("schema")) {
					schema.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("column")) {
					column.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("primaryKey")) {
					primaryKey.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("foreignKey")) {
					foreignKey.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("candidateKey")) {
					candidateKey.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("checkConstraint")) {
					checkConstraint.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("trigger")) {
					trigger.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("view")) {
					view.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("routine")) {
					routine.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("user")) {
					user.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("parameter")) {
					parameter.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("role")) {
					role.setDescription(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("privilege")) {
					privilege.setDescription(trimmedVal);
				}
			} else if (tag.equalsIgnoreCase("type")) {
				if (parentTag.equalsIgnoreCase("privilege")) {
					privilege.setType(trimmedVal);
				} else {
					type = createType(trimmedVal);
				}
			} else if (tag.equalsIgnoreCase("typeOriginal")) {
				type.setOriginalTypeName(trimmedVal);
				if (parentTag.equalsIgnoreCase("column")) {
					column.setType(type);
				} else if (parentTag.equalsIgnoreCase("parameter")) {
					parameter.setType(type);
				}
			} else if (tag.equalsIgnoreCase("defaultValue")) {
				column.setDefaultValue(trimmedVal);
			} else if (tag.equalsIgnoreCase("nullable")) {
				column.setNillable(Boolean.parseBoolean(trimmedVal));
			} else if (tag.equalsIgnoreCase("column")) {
				if (parentTag.equalsIgnoreCase("columns")) {
					columns.add(column);
				} else if (parentTag.equalsIgnoreCase("primaryKey")) {
					primaryKeyColumns.add(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("reference")) {
					reference.setColumn(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("candidateKey")) {
					candidateKeyColumns.add(trimmedVal);
				}
			} else if (tag.equalsIgnoreCase("columns")) {
				if (parentTag.equalsIgnoreCase("table")) {
					table.setColumns(columns);
				} else if (parentTag.equalsIgnoreCase("view")) {
					view.setColumns(columns);
				}
			} else if (tag.equalsIgnoreCase("primaryKey")) {
				primaryKey.setColumnNames(primaryKeyColumns);
				table.setPrimaryKey(primaryKey);
			} else if (tag.equalsIgnoreCase("referencedSchema")) {
				foreignKey.setReferencedSchema(trimmedVal);
			} else if (tag.equalsIgnoreCase("referencedTable")) {
				foreignKey.setReferencedTable(trimmedVal);
			} else if (tag.equalsIgnoreCase("referenced")) {
				reference.setReferenced(trimmedVal);
			} else if (tag.equalsIgnoreCase("reference")) {
				references.add(reference);

			} else if (tag.equalsIgnoreCase("matchType")) {
				foreignKey.setMatchType(trimmedVal);
			} else if (tag.equalsIgnoreCase("deleteAction")) {
				foreignKey.setDeleteAction(trimmedVal);
			} else if (tag.equalsIgnoreCase("updateAction")) {
				foreignKey.setUpdateAction(trimmedVal);
			} else if (tag.equalsIgnoreCase("foreignKey")) {
				foreignKey.setReferences(references);
				foreignKeys.add(foreignKey);
			} else if (tag.equalsIgnoreCase("foreignKeys")) {
				table.setForeignKeys(foreignKeys);
			} else if (tag.equalsIgnoreCase("candidateKey")) {
				candidateKey.setColumns(candidateKeyColumns);
				candidateKeys.add(candidateKey);
			} else if (tag.equalsIgnoreCase("candidateKeys")) {
				table.setCandidateKeys(candidateKeys);
			} else if (tag.equalsIgnoreCase("condition")) {
				checkConstraint.setCondition(trimmedVal);
			} else if (tag.equalsIgnoreCase("checkConstraint")) {
				checkConstraints.add(checkConstraint);
			} else if (tag.equalsIgnoreCase("checkConstraints")) {
				table.setCheckConstraints(checkConstraints);
			} else if (tag.equalsIgnoreCase("actionName")) {
				trigger.setActionTime(trimmedVal);
			} else if (tag.equalsIgnoreCase("triggerEvent")) {
				trigger.setTriggerEvent(trimmedVal);
			} else if (tag.equalsIgnoreCase("aliasList")) {
				trigger.setAliasList(trimmedVal);
			} else if (tag.equalsIgnoreCase("triggeredAction")) {
				trigger.setTriggeredAction(trimmedVal);
			} else if (tag.equalsIgnoreCase("trigger")) {
				triggers.add(trigger);
			} else if (tag.equalsIgnoreCase("triggers")) {
				table.setTriggers(triggers);
			} else if (tag.equalsIgnoreCase("rows")) {
				table.setRows(Integer.parseInt(trimmedVal));
			} else if (tag.equalsIgnoreCase("table")) {
				table.setSchema(schema);
				tables.add(table);
			} else if (tag.equalsIgnoreCase("tables")) {
				schema.setTables(tables);
			} else if (tag.equalsIgnoreCase("query")) {
				view.setQuery(trimmedVal);
			} else if (tag.equalsIgnoreCase("queryOriginal")) {
				view.setQueryOriginal(trimmedVal);
			} else if (tag.equalsIgnoreCase("view")) {
				views.add(view);
			} else if (tag.equalsIgnoreCase("views")) {
				schema.setViews(views);
			} else if (tag.equalsIgnoreCase("source")) {
				routine.setSource(trimmedVal);
			} else if (tag.equalsIgnoreCase("body")) {
				routine.setBody(trimmedVal);
			} else if (tag.equalsIgnoreCase("characteristic")) {
				routine.setCharacteristic(trimmedVal);
			} else if (tag.equalsIgnoreCase("returnType")) {
				routine.setReturnType(trimmedVal);
			} else if (tag.equalsIgnoreCase("mode")) {
				parameter.setMode(trimmedVal);
			} else if (tag.equalsIgnoreCase("parameter")) {
				parameters.add(parameter);
			} else if (tag.equalsIgnoreCase("parameters")) {
				routine.setParameters(parameters);
			} else if (tag.equalsIgnoreCase("routine")) {
				routines.add(routine);
			} else if (tag.equalsIgnoreCase("routines")) {
				schema.setRoutines(routines);
			} else if (tag.equalsIgnoreCase("schema")) {
				schemas.add(schema);
			} else if (tag.equalsIgnoreCase("schemas")) {
				dbStructure.setSchemas(schemas);
			} else if (tag.equalsIgnoreCase("user")) {
				users.add(user);
			} else if (tag.equalsIgnoreCase("users")) {
				dbStructure.setUsers(users);
			} else if (tag.equalsIgnoreCase("admin")) {
				role.setAdmin(trimmedVal);
			} else if (tag.equalsIgnoreCase("role")) {
				roles.add(role);
			} else if (tag.equalsIgnoreCase("roles")) {
				dbStructure.setRoles(roles);
			} else if (tag.equalsIgnoreCase("object")) {
				privilege.setObject(trimmedVal);
			} else if (tag.equalsIgnoreCase("grantor")) {
				privilege.setGrantor(trimmedVal);
			} else if (tag.equalsIgnoreCase("grantee")) {
				privilege.setGrantee(trimmedVal);
			} else if (tag.equalsIgnoreCase("option")) {
				privilege.setOption(trimmedVal);
			} else if (tag.equalsIgnoreCase("privilege")) {
				privileges.add(privilege);
			} else if (tag.equalsIgnoreCase("privileges")) {
				dbStructure.setPrivileges(privileges);
			}
		}

		@Override
		public void characters(char buf[], int offset, int len) {
			tempVal.append(buf, offset, len);
		}

		private void pushTag(String tag) {
			tagsStack.push(tag);
		}

		private String popTag() {
			return tagsStack.pop();
		}

		private String peekTag() {
			return tagsStack.peek();
		}

		private Type createType(String sqlType) {
			sqlType = sqlType.toUpperCase();
			logger.debug("sqlType: " + sqlType);
			Type type = null;

			if (sqlType.startsWith("INT")) {
				type = new SimpleTypeNumericExact(10, 0);
				type.setSql99TypeName("INTEGER");
			} else if (sqlType.equals("SMALLINT")) {
				type = new SimpleTypeNumericExact(5, 0);
				type.setSql99TypeName("SMALLINT");
			} else if (sqlType.startsWith("NUMERIC")) {
				type = new SimpleTypeNumericExact(getPrecision(sqlType),
						getScale(sqlType));
				type.setSql99TypeName("NUMERIC");
			} else if (sqlType.startsWith("DEC")) {
				type = new SimpleTypeNumericExact(getPrecision(sqlType),
						getScale(sqlType));
				type.setSql99TypeName("DECIMAL");
			} else if (sqlType.equals("FLOAT")) {
				type = new SimpleTypeNumericApproximate(53);
				type.setSql99TypeName("FLOAT");
			} else if (sqlType.startsWith("FLOAT")) {
				type = new SimpleTypeNumericApproximate(getPrecision(sqlType));
				type.setSql99TypeName("FLOAT");
			} else if (sqlType.equals("REAL")) {
				type = new SimpleTypeNumericApproximate(24);
				type.setSql99TypeName("REAL");
			} else if (sqlType.startsWith("DOUBLE")) {
				type = new SimpleTypeNumericApproximate(53);
				type.setSql99TypeName("DOUBLE PRECISION");
			} else if (sqlType.equals("BIT")) {
				type = new SimpleTypeBoolean();
				type.setSql99TypeName("BOOLEAN");
			} else if (sqlType.startsWith("BIT VARYING")) {
				 type = new SimpleTypeBinary(getLength(sqlType));
				 type.setSql99TypeName("BIT VARYING");
			} else if (sqlType.startsWith("BIT")) {
				if (getLength(sqlType) == 1) {
					type = new SimpleTypeBoolean();
					type.setSql99TypeName("BOOLEAN");
				} else {
					type = new SimpleTypeBinary(getLength(sqlType));
					type.setSql99TypeName("BIT");
				}
			} else if (sqlType.startsWith("BINARY LARGE OBJECT")
					|| sqlType.startsWith("BLOB")) {
				type = new SimpleTypeBinary();
				type.setSql99TypeName("BINARY LARGE OBJECT");
			} else if (sqlType.startsWith("CHAR")) {
				if (isLargeObject(sqlType)) {
					type = new SimpleTypeString(getCLOBMinimum(), Boolean.TRUE);
					type.setSql99TypeName("CHARACTER LARGE OBJECT");
				} else {
					if (isLengthVariable(sqlType)) {
						type = new SimpleTypeString(getLength(sqlType),
								Boolean.TRUE);
								type.setSql99TypeName("CHARACTER VARYING");
					} else {
						type = new SimpleTypeString(getLength(sqlType),
								Boolean.FALSE);
								type.setSql99TypeName("CHARACTER");
					}
				}
			} else if (sqlType.startsWith("VARCHAR")) {
				type = new SimpleTypeString(getLength(sqlType), Boolean.TRUE);
				type.setSql99TypeName("CHARACTER VARYING");
			} else if (sqlType.startsWith("NATIONAL")) {
				if (isLargeObject(sqlType) || sqlType.startsWith("NCLOB")) {
					type = new SimpleTypeString(getCLOBMinimum(),
							Boolean.TRUE, ENCODING);
					type.setSql99TypeName("NATIONAL CHARACTER LARGE OBJECT");
				} else {
					if (isLengthVariable(sqlType)) {
						type = new SimpleTypeString(getLength(sqlType),
								Boolean.TRUE, ENCODING);
						type.setSql99TypeName("NATIONAL CHARACTER VARYING");
					} else {
						type = new SimpleTypeString(getLength(sqlType),
								Boolean.FALSE, ENCODING);
						type.setSql99TypeName("NATIONAL CHARACTER");
					}
				}
			} else if (sqlType.equals("BOOLEAN")) {
				type = new SimpleTypeBoolean();
				type.setSql99TypeName("BOOLEAN");
			} else if (sqlType.equals("DATE")) {
				type = new SimpleTypeDateTime(Boolean.FALSE, Boolean.FALSE);
				type.setSql99TypeName("DATE");
			}  else if (sqlType.equals("TIMESTAMP WITH TIME ZONE")) {
				type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
				type.setSql99TypeName("TIMESTAMP WITH TIME ZONE");
			}  else if (sqlType.equals("TIMESTAMP")) {
				type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
				type.setSql99TypeName("TIMESTAMP");
			} else if (sqlType.equals("TIME WITH TIME ZONE")) {
				type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
				type.setSql99TypeName("TIME WITH TIME ZONE");
			} else if (sqlType.equals("TIME")) {
				type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
				type.setSql99TypeName("TIME");
			} else {
				type = new SimpleTypeString(255, Boolean.TRUE);
				type.setSql99TypeName("CHARACTER VARYING");
			}

			return type;
		}

		private int getCLOBMinimum() {
			return 65535;
		}

		private int getLength(String sqlType) {
			int length = -1;
			int start = sqlType.indexOf("(");
			int end = sqlType.indexOf(")");

			if (start < 0) {
				length = 1;
			} else {
				length = Integer.parseInt(sqlType.substring(start + 1, end));
			}
			return length;
		}

		@SuppressWarnings("unused")
		private int getLengthLarge(String sqlType) {
			int length = -1;
			int multiplier = -1;
			int start = sqlType.indexOf("(");
			int end = sqlType.indexOf(")");

			if (start < 0) {
				length = 1024;
			} else {
				String sub = sqlType.substring(start + 1, end);
				StringBuilder sb = new StringBuilder(sub);

				if (sub.contains("K")) {
					multiplier = 1024;
					sb.deleteCharAt(sub.indexOf("K"));
				} else if (sub.contains("M")) {
					multiplier = 1024 * 1024;
					sb.deleteCharAt(sub.indexOf("M"));
				} else if (sub.contains("G")) {
					multiplier = 1024 * 1024 * 1024;
					sb.deleteCharAt(sub.indexOf("G"));
				} else {
					multiplier = 1;
				}

				sub = sb.toString();
				length = Integer.parseInt(sub) * multiplier;
			}
			return length;
		}

		private int getPrecision(String sqlType) {
			int precision = -1;
			int start = sqlType.indexOf("(");
			int end = sqlType.indexOf(",");

			if (end < 0) {
				end = sqlType.indexOf(")");
			}

			if (start < 0) {
				precision = 1;
			} else {
				precision = Integer.parseInt(sqlType.substring(start + 1, end));
			}
			return precision;
		}

		private int getScale(String sqlType) {
			int scale = -1;
			int start = sqlType.indexOf(",");
			int end = sqlType.indexOf(")");
			if (start < 0) {
				scale = 0;
			} else {
				scale = Integer.parseInt(sqlType.substring(start + 1, end));
			}
			return scale;
		}

		private boolean isLengthVariable(String sqlType) {
			return sqlType.contains("VARYING");
		}

		private boolean isLargeObject(String sqlType) {
			return (sqlType.contains("LARGE OBJECT")
					|| sqlType.contains("LOB"));
		}

		protected DatabaseStructure getDatabaseStructure() {
			return dbStructure;
		}
	}


	public class SIARDContentSAXHandler extends DefaultHandler {

		private DatabaseHandler handler;
		private TableStructure currentTable;
		private BinaryCell currentBinaryCell;
		private Map<String, Throwable> errors;

		private final Stack<String> tagsStack = new Stack<String>();
		private final StringBuilder tempVal = new StringBuilder();

		private Row row;
		private List<Cell> cells;
		private int rowIndex;

		public SIARDContentSAXHandler(DatabaseHandler handler) {
			this.handler = handler;
			this.errors = new TreeMap<String, Throwable>();
		}

		public Map<String, Throwable> getErrors() {
			return errors;
		}

		@Override
		public void startDocument() throws SAXException {
			pushTag("");
		}

		@Override
		public void endDocument() throws SAXException {
			// nothing to do
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attr) {
			pushTag(qName);
			tempVal.setLength(0);

			if (qName.equalsIgnoreCase("table")) {
				this.rowIndex = 0;
				try {
					handler.handleDataOpenTable(currentTable.getSchema(),currentTable.getId());
				} catch (ModuleException e) {
					logger.error("An error occurred "
							+ "while handling data open table", e);
				}
			} else if (qName.equalsIgnoreCase("row")) {
				row = new Row();
				cells = new ArrayList<Cell>();
				for (int i = 0; i < currentTable.getColumns().size(); i++) {
					cells.add(new SimpleCell(""));
				}
			} else if (qName.startsWith("c")) {
				if (attr.getValue("file") != null) {
					logger.debug("<c> binary " + attr.getValue("file"));
					String fileDir = attr.getValue("file");
					ZipArchiveEntry lob = zipFile.getEntry(fileDir);
					if (lob == null) {
						errors.put("Could not find lob in '" + fileDir + "'",
								null);
					}
					InputStream stream;
					FileItem fileItem;
					try {
						stream = zipFile.getInputStream(lob);
						fileItem =
								(stream != null) ? new FileItem(stream) : null;
						currentBinaryCell = new BinaryCell(fileDir, fileItem);

					}
					catch (IOException e) {
						errors.put("Failed to get InputStream of "
								+ "ZipArchiveEntry", e);
					}
					catch (ModuleException e) {
						errors.put("Failed to create new FileItem", e);
					}
				} else {
					currentBinaryCell = null;
				}
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			String tag = peekTag();
			if (!qName.equals(tag)) {
				throw new InternalError();
			}

			popTag();
			String trimmedVal = tempVal.toString().trim();

			if (tag.equalsIgnoreCase("table")) {
				try {
					logger.debug("before handle data close");
					handler.handleDataCloseTable(currentTable.getSchema(), currentTable.getId());
				} catch (ModuleException e) {
					logger.error("An error occurred "
							+ "while handling data close table", e);
				}
			} else if (tag.equalsIgnoreCase("row")) {
				row.setIndex(rowIndex);
				row.setCells(cells);
				try {
					handler.handleDataRow(row);
				} catch (InvalidDataException e) {
					logger.error(
							"An error occurred while handling data row", e);
				} catch (ModuleException e) {
					logger.error(
							"An error occurred while handling data row", e);
				} finally {
					this.rowIndex++;
				}
			} else if (tag.contains("c")) {
				// TODO Support other cell types
				String[] subStrings = tag.split("c");
				Integer colIndex = Integer.valueOf(subStrings[1]);
				Type type =
						currentTable.getColumns().get(colIndex-1).getType();
				if (type instanceof SimpleTypeString) {
					trimmedVal = SIARDHelper.decode(trimmedVal);
				}

				Cell cell = null;
				String id = currentTable.getId() + "."
						+ currentTable.getColumns().get(colIndex-1).getName()
						+ "." + colIndex;
				if (currentBinaryCell != null) {
					cell = currentBinaryCell;
				} else if (type instanceof SimpleTypeBinary) {
					/*
					 * in case:
					 *   - binary cell < 2000 bytes (does not have its own file)
					 *   - binary cell is null
					 */
					InputStream is = new ByteArrayInputStream(SIARDHelper.
							hexStringToByteArray(trimmedVal));
					try {
						cell = new BinaryCell(id, new FileItem(is));
					} catch (ModuleException e) {
						logger.error("An error occurred while importing "
								+ "in-table binary celll");
					}
				} else {
					cell = new SimpleCell(id);
					if (trimmedVal.length() > 0) {
						// logger.debug("trimmed: " + trimmedVal);
						((SimpleCell) cell).setSimpledata(trimmedVal);
					} else {
						((SimpleCell) cell).setSimpledata(null);
					}
				}
				cells.set(colIndex-1, cell);
			}
		}

		@Override
		public void characters(char buf[], int offset, int len) {
			tempVal.append(buf, offset, len);
		}

		private void pushTag(String tag) {
			tagsStack.push(tag);
		}

		private String popTag() {
			return tagsStack.pop();
		}

		private String peekTag() {
			return tagsStack.peek();
		}

		public void setCurrentTable(TableStructure table) {
			currentTable = table;
		}
	}
}
