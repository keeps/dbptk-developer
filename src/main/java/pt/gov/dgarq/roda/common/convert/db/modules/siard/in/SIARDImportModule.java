package pt.gov.dgarq.roda.common.convert.db.modules.siard.in;

import java.io.File;
import java.io.FileInputStream;
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

import pt.gov.dgarq.roda.common.convert.db.model.exception.InvalidDataException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDCandidateKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDCheckConstraint;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDDatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDForeignKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDPrimaryKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDReference;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDRoutineStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDSchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDTableStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDTrigger;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDUserStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SIARDViewStructure;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseHandler;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseImportModule;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDImportModule implements DatabaseImportModule {
	
	public static final String SIARD_DEFAULT_FILE_NAME = "Default.siard";
	
	private static final String SCHEMA_VERSION = "UNKNONW";
	
	private final Logger logger = Logger.getLogger(SIARDImportModule.class);
	
	private SAXParser saxParser;
	
	private InputStream metadata; 
	
	private InputStream siard;
	
	
//	public SIARDImportModule(InputStream siard, SIARDBinaryLookup binLookup)
//			throws ModuleException {
//		init(siard, binLookup);
//	}
	
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
		
			// FIXME get files from zip
			ZipFile zipFile = new ZipFile(siardPackage);
			ZipArchiveEntry metadata = zipFile.getEntry("header/metadata.xml");
			
			//InputStream metadataInputStream = zipFile.getInputStream(metadata);
			
			InputStream metadataInputStream = new FileInputStream(new File("/Users/miguelcoutada/Desktop/metadata.xml"));
			init(metadataInputStream);
			zipFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public void init(InputStream metadata) throws ModuleException {
		this.metadata = metadata;
		
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		try {
			this.saxParser = saxParserFactory.newSAXParser();
		} catch (SAXException e) {
			throw new ModuleException("Error initializing SAX parser", e);
		} catch (ParserConfigurationException e) {
			throw new ModuleException("Error initializing SAX parser", e);
		}
	}
	
	@Override
	public void getDatabase(DatabaseHandler databaseHandler)
			throws ModuleException, UnknownTypeException, InvalidDataException {
		SIARDSAXHandler siardSAXHandler = new SIARDSAXHandler();
		try {
			saxParser.parse(metadata, siardSAXHandler);
			// TODO parse content
		} catch (SAXException e) {
			throw new ModuleException("Error parsing SIARD", e);
		} catch (IOException e) {
			throw new ModuleException("Error reading SIARD", e);
		}
	}
	
	public class SIARDSAXHandler extends DefaultHandler {
		
		private final Stack<String> tagsStack = new Stack<String>();
		private final StringBuilder tempVal = new StringBuilder();
				
		private SIARDDatabaseStructure dbStructure;
		private List<SIARDSchemaStructure> schemas;
		private SIARDSchemaStructure schema;
		private List<SIARDTableStructure> tables;
		private SIARDTableStructure table;
		private List<SIARDColumnStructure> columns;
		private SIARDColumnStructure column;
		private SIARDPrimaryKey primaryKey;
		private List<String> primaryKeyColumns;
		private List<SIARDForeignKey> foreignKeys;
		private SIARDForeignKey foreignKey;
		private SIARDReference reference;
		private List<SIARDCandidateKey> candidateKeys;
		private List<String> candidateKeyColumns;
		private SIARDCandidateKey candidateKey;
		private List<SIARDCheckConstraint> checkConstraints;
		private SIARDCheckConstraint checkConstraint;
		private List<SIARDTrigger> triggers;
		private SIARDTrigger trigger;
		private List<SIARDViewStructure> views;
		private SIARDViewStructure view;
		private List<SIARDRoutineStructure> routines;
		private SIARDRoutineStructure routine;
		private List<SIARDUserStructure> users;
		private SIARDUserStructure user;
		// TODO add roles & privileges
		//private List<SIARDRoleStructure> roles;
		//private List<SIARDPrivilegesStructure> privileges;
				
		public SIARDSAXHandler() {
			dbStructure = null;
		}
		
		public void startDocument() {
			pushTag("");
		}
		
		public void endDocument() {
			logger.debug(dbStructure.toString());			
		}
	
		public void startElement(String uri, String localName, String qName,
				Attributes attr) {	
			pushTag(qName);
			tempVal.setLength(0);

			if (qName.equalsIgnoreCase("siardArchive")) {
				dbStructure = new SIARDDatabaseStructure();
				dbStructure.setVersion(attr.getValue("version"));
			} else if (qName.equalsIgnoreCase("schemas")) {
				schemas = new ArrayList<SIARDSchemaStructure>();
			} else if (qName.equalsIgnoreCase("schema")) {
				schema = new SIARDSchemaStructure();
			} else if (qName.equalsIgnoreCase("tables")) {
				tables = new ArrayList<SIARDTableStructure>();
			} else if (qName.equalsIgnoreCase("table")) {
				table = new SIARDTableStructure();
			} else if (qName.equalsIgnoreCase("columns")) {
				columns = new ArrayList<SIARDColumnStructure>();
			} else if (qName.equalsIgnoreCase("column")) {
					column = new SIARDColumnStructure();
			} else if (qName.equalsIgnoreCase("primaryKey")) {
				primaryKey = new SIARDPrimaryKey();
				primaryKeyColumns = new ArrayList<String>();
			} else if (qName.equalsIgnoreCase("foreignKeys")) {
				foreignKeys = new ArrayList<SIARDForeignKey>();
			} else if (qName.equalsIgnoreCase("foreignKey")) {
				foreignKey = new SIARDForeignKey();
			} else if (qName.equalsIgnoreCase("reference")) {
				reference = new SIARDReference();
			} else if (qName.equalsIgnoreCase("candidateKeys")) {
				candidateKeys = new ArrayList<SIARDCandidateKey>();
			} else if (qName.equalsIgnoreCase("candidateKey")) {
				candidateKey = new SIARDCandidateKey();
				candidateKeyColumns = new ArrayList<String>();
			} else if (qName.equalsIgnoreCase("checkConstraints")) {
				checkConstraints = new ArrayList<SIARDCheckConstraint>();
			} else if (qName.equalsIgnoreCase("checkConstraint")) {
				checkConstraint = new SIARDCheckConstraint();
			} else if (qName.equalsIgnoreCase("triggers")) {
				triggers = new ArrayList<SIARDTrigger>();
			} else if (qName.equalsIgnoreCase("trigger")) {
				trigger = new SIARDTrigger();
			} else if (qName.equalsIgnoreCase("view")) {
				view = new SIARDViewStructure();
			} else if (qName.equalsIgnoreCase("views")) {
				views = new ArrayList<SIARDViewStructure>();
			} else if (qName.equalsIgnoreCase("routine")) {
				routine = new SIARDRoutineStructure();
			} else if (qName.equalsIgnoreCase("views")) {
				routines = new ArrayList<SIARDRoutineStructure>();
			} else if (qName.equalsIgnoreCase("user")) {
				user = new SIARDUserStructure();
			} else if (qName.equalsIgnoreCase("users")) {
				users = new ArrayList<SIARDUserStructure>();
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
				dbStructure.setDbname(trimmedVal);	
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
				// FIXME
				//dbStructure.setArchivalDate(trimmedVal);
			} else if (tag.equalsIgnoreCase("messageDigest")) {
				dbStructure.setMessageDigest(trimmedVal);
			} else if (tag.equalsIgnoreCase("clientMachine")) {
				dbStructure.setClientMachine(trimmedVal);
			} else if (tag.equalsIgnoreCase("databaseProduct")) {
				dbStructure.setDatabaseProduct(trimmedVal);
			} else if (tag.equalsIgnoreCase("connection")) {
				dbStructure.setConnection(trimmedVal);
			} else if (tag.equalsIgnoreCase("databaseUser")) {
				dbStructure.setDatabaseUser(trimmedVal);
			} else if (tag.equalsIgnoreCase("name")) {
				if (parentTag.equalsIgnoreCase("table")) {
					table.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("schema")) {
					schema.setName(trimmedVal);
				} else if (parentTag.equalsIgnoreCase("column")) {
					column.setName(trimmedVal);
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
				column.setType(trimmedVal);
			} else if (tag.equalsIgnoreCase("typeOriginal")) {
				column.setTypeOriginal(trimmedVal);
			} else if (tag.equalsIgnoreCase("defaultValue")) {
				column.setDefaultValue(trimmedVal);
			} else if (tag.equalsIgnoreCase("nullable")) {
				column.setNullable(Boolean.parseBoolean(trimmedVal));
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
				primaryKey.setColumns(primaryKeyColumns);
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
				// TODO add triggers
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

	}

}
