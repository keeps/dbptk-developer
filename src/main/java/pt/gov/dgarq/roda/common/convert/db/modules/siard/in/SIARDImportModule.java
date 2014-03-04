package pt.gov.dgarq.roda.common.convert.db.modules.siard.in;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.data.Row;
import pt.gov.dgarq.roda.common.convert.db.model.data.SimpleCell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.InvalidDataException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.CandidateKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.CheckConstraint;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ForeignKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.PrimaryKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.Reference;
import pt.gov.dgarq.roda.common.convert.db.model.structure.RoutineStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.Trigger;
import pt.gov.dgarq.roda.common.convert.db.model.structure.UserStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ViewStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBoolean;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseHandler;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseImportModule;

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
			this.header = zipFile.getInputStream(metadata);
		} catch (IOException e) {
			throw new ModuleException("An error ocurred while accessing to "
					+ "a file in SIARD package", e);
		}
	}
	
	protected void setCurrentInputStream(SchemaStructure schema, 
			TableStructure table) throws ModuleException {
		try {
			ZipArchiveEntry content = zipFile.getEntry("content/"
					+ schema.getFolder() + "/" + table.getFolder() 
					+ "/" + table.getFolder() + ".xml");
			
			this.currentInputStream = zipFile.getInputStream(content);
		} catch (IOException e) {
			throw new ModuleException("An error ocurred while accessing to "
					+ "a file in SIARD package", e);
		}
	}
	
	@Override
	public void getDatabase(DatabaseHandler handler)
			throws ModuleException, UnknownTypeException, InvalidDataException {
		SIARDHeaderSAXHandler siardHeaderSAXHandler = 
				new SIARDHeaderSAXHandler(handler);
		SIARDContentSAXHandler siardContentSAXHandler = 
				new SIARDContentSAXHandler(handler);
		
		try {
			setHeader();
			saxParser.parse(header, siardHeaderSAXHandler);
			header.close();
			
			dbStructure = siardHeaderSAXHandler.getDatabaseStructure();
			SchemaStructure schema = dbStructure.getSchemas().get(0);
			//for (SchemaStructure schema : dbStructure.getSchemas()) {
				for (TableStructure table : schema.getTables()) {
					setCurrentInputStream(schema, table);
					siardContentSAXHandler.setCurrentTable(table);
					logger.debug("Parsing " + schema.getName() + "/"
							+ table.getName());
					saxParser.parse(currentInputStream, siardContentSAXHandler);
					currentInputStream.close();
				}
			//}
			zipFile.close();
			
		} catch (SAXException e) {
			throw new ModuleException("Error parsing SIARD", e);
		} catch (IOException e) {
			throw new ModuleException("Error reading SIARD", e);
		}
	}
	
	public class SIARDHeaderSAXHandler extends DefaultHandler {
		
		private DatabaseHandler handler;
		
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
		private List<UserStructure> users;
		private UserStructure user;
		// TODO add roles & privileges
		//private List<SIARDRoleStructure> roles;
		//private List<SIARDPrivilegesStructure> privileges;
				
		public SIARDHeaderSAXHandler(DatabaseHandler handler) {
			this.handler = handler;
			// TODO set vars to null
		}
		
		public void startDocument() {
			pushTag("");
		}
		
		public void endDocument() {
			logger.debug(dbStructure.toString());
			try {
				handler.handleStructure(dbStructure);
			} catch (ModuleException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		public void startElement(String uri, String localName, String qName,
				Attributes attr) {	
			pushTag(qName);
			tempVal.setLength(0);

			if (qName.equalsIgnoreCase("siardArchive")) {
				dbStructure = new DatabaseStructure();
				//dbStructure.setVersion(attr.getValue("version"));
				// TODO handle siard format version;
			} else if (qName.equalsIgnoreCase("schemas")) {
				schemas = new ArrayList<SchemaStructure>();
			} else if (qName.equalsIgnoreCase("schema")) {
				schema = new SchemaStructure();
			} else if (qName.equalsIgnoreCase("tables")) {
				tables = new ArrayList<TableStructure>();
			} else if (qName.equalsIgnoreCase("table")) {
				table = new TableStructure();
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
			} else if (qName.equalsIgnoreCase("view")) {
				view = new ViewStructure();
			} else if (qName.equalsIgnoreCase("views")) {
				views = new ArrayList<ViewStructure>();
			} else if (qName.equalsIgnoreCase("routine")) {
				routine = new RoutineStructure();
			} else if (qName.equalsIgnoreCase("views")) {
				routines = new ArrayList<RoutineStructure>();
			} else if (qName.equalsIgnoreCase("user")) {
				user = new UserStructure();
			} else if (qName.equalsIgnoreCase("users")) {
				users = new ArrayList<UserStructure>();
			}
			// TODO add roles & privileges
		}
		
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
				dbStructure.setCreationDate(trimmedVal);
			} else if (tag.equalsIgnoreCase("messageDigest")) {
				dbStructure.setMessageDigest(trimmedVal);
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
					table.setId(trimmedVal);
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
				}
			} else if (tag.equalsIgnoreCase("folder")) {
				if (parentTag.equalsIgnoreCase("table")) {
					table.setFolder(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("schema")) {
					schema.setFolder(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("column")) {
					column.setFolder(trimmedVal);
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
				}
			} else if (tag.equalsIgnoreCase("type")) {
				type = createType(trimmedVal);
			} else if (tag.equalsIgnoreCase("typeOriginal")) {
				type.setOriginalTypeName(trimmedVal);
				column.setType(type);
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
				foreignKey.setReference(reference);
			} else if (tag.equalsIgnoreCase("matchType")) {
				foreignKey.setMatchType(trimmedVal);
			} else if (tag.equalsIgnoreCase("deleteAction")) {
				foreignKey.setDeleteAction(trimmedVal);
			} else if (tag.equalsIgnoreCase("updateAction")) {
				foreignKey.setUpdateAction(trimmedVal);
			} else if (tag.equalsIgnoreCase("foreignKey")) {
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
			} else if (tag.equalsIgnoreCase("parameters")) {
				routine.setParameters(trimmedVal);
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
			}
			// TODO add roles
			// TODO add privileges
		}
		
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
			// FIXME complete
			sqlType = sqlType.toUpperCase();
			Type type = null;
						
			if (sqlType.startsWith("INT")) {
				type = new SimpleTypeNumericExact(10, 0);
			} else if (sqlType.equals("SMALLINT")) {
				type = new SimpleTypeNumericExact(5, 0);
			} else if (sqlType.startsWith("NUMERIC")
					|| sqlType.startsWith("DEC")) {
				type = new SimpleTypeNumericExact(getPrecision(sqlType),
						getScale(sqlType));
			} else if (sqlType.equals("FLOAT")) {
				type = new SimpleTypeNumericApproximate(53);
			} else if (sqlType.startsWith("FLOAT")) {
				type = new SimpleTypeNumericApproximate(getPrecision(sqlType));
			} else if (sqlType.equals("REAL")) {
				type = new SimpleTypeNumericApproximate(24);
			} else if (sqlType.startsWith("DOUBLE")) {
				type = new SimpleTypeNumericApproximate(53);
			} else if (sqlType.equals("BIT")) {
				type = new SimpleTypeBoolean();
			} else if (sqlType.startsWith("BIT")) {
				// FIXME bit string
				if (getLength(sqlType) == 1) {
					type = new SimpleTypeBoolean();
				} else if (isLengthVariable(sqlType)) {
					// FIXME simple type bit variable
					type = new SimpleTypeBinary();
				} else {
					// FIXME simple type bit not variable
					type = new SimpleTypeBinary();
				}
			} else if (sqlType.startsWith("BINARY LARGE OBJECT")
					|| sqlType.startsWith("BLOB")) {
				// FIXME length
				type = new SimpleTypeBinary();
			} else if (sqlType.startsWith("CHAR")) {
				if (isLargeObject(sqlType)) {
					type = new SimpleTypeString(getLengthLarge(sqlType), 
							Boolean.TRUE);
				} else {
					type = new SimpleTypeString(getLength(sqlType), 
							isLengthVariable(sqlType));
				}
			} else if (sqlType.startsWith("VARCHAR")) {
				type = new SimpleTypeString(
						getLength(sqlType), Boolean.TRUE);
			} else if (sqlType.startsWith("NATIONAL")) {
				if (isLargeObject(sqlType) || sqlType.startsWith("NCLOB")) {
					type = new SimpleTypeString(getLengthLarge(sqlType), 
							Boolean.TRUE, ENCODING);
				}
				type = new SimpleTypeString(getLength(sqlType), 
						isLengthVariable(sqlType), ENCODING);
			} else if (sqlType.equals("BOOLEAN")) {
				type = new SimpleTypeBoolean();
			} else if (sqlType.equals("DATE")) {
				type = new SimpleTypeDateTime(Boolean.FALSE, Boolean.FALSE);
			} else if (sqlType.equals("TIMESTAMP")) {
				type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
			} else if (sqlType.equals("TIME")) {
				type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
			} else {
				logger.debug("last!: " + sqlType);
				type = new SimpleTypeString(255, Boolean.TRUE);
			}
			 			
			return type;
		}
		
		private int getLength(String sqlType) {
			logger.debug(sqlType);
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
		
		private int getLengthLarge(String sqlType) {
			logger.debug(sqlType);
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
			logger.debug("type: " + sqlType);
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
		
		private final Stack<String> tagsStack = new Stack<String>();		
		private final StringBuilder tempVal = new StringBuilder();
		
		private Row row;
		private List<Cell> cells;
		private int colIndex;
		
		
		public SIARDContentSAXHandler(DatabaseHandler handler) {
			this.handler = handler;
		}
		
		public void startDocument() throws SAXException {
			pushTag("");
		}
		
		public void endDocument() throws SAXException {
			logger.debug("Ended. Table content summary: HERE");
		}
		
		public void startElement(String uri, String localName, String qName,
				Attributes attr) {	
			pushTag(qName);
			tempVal.setLength(0);
			
			if (qName.equalsIgnoreCase("table")) {
				try {
					handler.handleDataOpenTable(currentTable.getId());
				} catch (ModuleException e) {
					// TODO Treat error..
					e.printStackTrace();
				}
			}
			else if (qName.equalsIgnoreCase("row")) {
				row = new Row();
				cells = new ArrayList<Cell>();
				colIndex = 0;
			} 
		}

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
					handler.handleDataCloseTable(currentTable.getId());
				} catch (ModuleException e) {
					// TODO treat error..
					e.printStackTrace();
				}
				logger.debug("--------End Table--------");
			} else if (tag.equalsIgnoreCase("row")) {
				row.setIndex(colIndex);
				row.setCells(cells);
				try {
					logger.debug("rows index: " + row.getIndex());
					for (Cell c : row.getCells()) {
						logger.debug("cell id: " + c.getId());						
					}
					handler.handleDataRow(row);
				} catch (InvalidDataException e) {
					// TODO treat errors
					e.printStackTrace();
				} catch (ModuleException e) {
					// TODO treat errors
					e.printStackTrace();
				}
				logger.debug("--End Row--");
			} else if (tag.contains("c")) {
				// TODO treat cell types
				String[] subStrings = tag.split("c");
				Integer colIndex = Integer.valueOf(subStrings[1]);
				
				SimpleCell simpleCell = new SimpleCell(currentTable.getId() 
						+ "." 
						+ currentTable.getColumns().get(colIndex-1).getName()  
						+ "." + colIndex);
				simpleCell.setSimpledata(trimmedVal);
				cells.add(simpleCell);
				logger.debug("TAG C" + colIndex + ": " + trimmedVal);
				this.colIndex++;
			}
		}

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
