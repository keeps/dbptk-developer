package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.Main;
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
import pt.gov.dgarq.roda.common.convert.db.model.structure.Parameter;
import pt.gov.dgarq.roda.common.convert.db.model.structure.PrimaryKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.PrivilegeStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.Reference;
import pt.gov.dgarq.roda.common.convert.db.model.structure.RoleStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.RoutineStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.Trigger;
import pt.gov.dgarq.roda.common.convert.db.model.structure.UserStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ViewStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.ComposedTypeArray;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.ComposedTypeStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBoolean;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseHandler;
import pt.gov.dgarq.roda.common.convert.db.modules.siard.SIARDHelper;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportModule implements DatabaseHandler {

	private final Logger logger = Logger.getLogger(SIARDExportModule.class);
	
	// private static final String DEFAULT_SIARD_PACKAGE_NAME = "export.siard";; 
	
	private static final String ENCODING = "UTF-8";
	
	private ZipArchiveOutputStream zipOut;
				
	private DatabaseStructure dbStructure; 
	
	/**
	 * 
	 * @param siardPackage
	 * @throws FileNotFoundException
	 */
	public SIARDExportModule(File siardPackage) 
		throws FileNotFoundException {
			
		try {
			this.zipOut = new ZipArchiveOutputStream(siardPackage);
			zipOut.setUseZip64(Zip64Mode.Always);
		} catch (IOException e) {
			logger.error("Error while creating SIARD archive file", e);
		}		
	}

	@Override
	public void initDatabase() throws ModuleException {
		// nothing to do
	}
	
	@Override 
	public void setIgnoredSchemas(Set<String> ignoredSchemas) {
		// nothing to do
	}

	@Override
	public void handleStructure(DatabaseStructure structure)
			throws ModuleException, UnknownTypeException {
		dbStructure = structure;	
	}

	@Override
	public void handleDataOpenTable(String tableId) throws ModuleException {
		if (dbStructure == null) {
			throw new ModuleException(
					"Database structure handling was not performed");
		}
		
		TableStructure table = dbStructure.lookupTableStructure(tableId);
		if (table == null) {
			throw new ModuleException(
					"Couldn't find table with id: " + tableId);
		}
		
		ArchiveEntry archiveEntry = new ZipArchiveEntry("content/" 
				+ table.getSchema().getFolder() + "/" + table.getFolder() 
				+ "/" + table.getFolder() + ".xml");
		
		try {
			zipOut.putArchiveEntry(archiveEntry);
			exportDataOpenTable();
		} catch (IOException e) {
			throw new ModuleException("Error handling data open table " 
					+ tableId, e);
		}
	}

	@Override
	public void handleDataCloseTable(String tableId) throws ModuleException {		
		try {
			exportDataCloseTable();
			zipOut.closeArchiveEntry();
		} catch (IOException e) {
			throw new ModuleException("Error closing table " + tableId, e);
		}		
	}


	@Override
	public void handleDataRow(Row row) throws InvalidDataException,
			ModuleException {
		try {
			exportRowData(row);
		} catch (IOException e) {
			throw new ModuleException(
					"Error exporting row " + row.getIndex(), e);
		}		
	}

	@Override
	public void finishDatabase() throws ModuleException {
		ArchiveEntry metaXML = new ZipArchiveEntry("header/metadata.xml");
		try {
			zipOut.putArchiveEntry(metaXML);
			try {
				exportDatabaseStructure(dbStructure);
			} catch (UnknownTypeException e) {
				logger.error("Error handling database structure", e);
			}
			zipOut.closeArchiveEntry();
		} catch (IOException e) {
			logger.error("Error while writing database "
					+ "structure to SIARD package", e);
		}
		
		ArchiveEntry metaXSD = new ZipArchiveEntry("header/metadata.xsd");
		try {
			zipOut.putArchiveEntry(metaXSD);
			zipOut.write(IOUtils.toByteArray(
					getClass().getResourceAsStream("/schema/siard.xsd")));
			zipOut.closeArchiveEntry();
		} catch (IOException e) {
			logger.error(
					"Error while writing metadata.xsd to SIARD package", e);
		}
		
		try {
			zipOut.finish();
			zipOut.close();
		} catch (IOException e) {
			logger.error("Error while closing SIARD archive file");
		}		
	}
	
	private void exportDatabaseStructure(DatabaseStructure structure) 
			throws IOException, ModuleException, UnknownTypeException {
		print("<?xml version=\"1.0\" encoding=\"" + ENCODING + "\"?>\n");
		// FIXME change to appropriate header 
		print("<siardArchive version=\"" + "1.0" + "\" "
				+ "xmlns=\"http://www.bar.admin.ch/xmlns/siard/1.0/metadata.xsd\" "
				+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
				+ "xsi:schemaLocation=\"http://www.bar.admin.ch/xmlns/siard/1.0/metadata.xsd metadata.xsd\""
				+ ">\n"); 
		
		if (structure.getName() != null) {
			print("\t<dbname>" + structure.getName() + "</dbname>\n");
		} else {
			throw new ModuleException("Error while exporting structure: "
					+ "dbname cannot be null");
		}
		if (structure.getDescription() != null) {
			print("\t<description>" + structure.getDescription() 
					+ "</description>\n");
		}
		if (structure.getArchiver() != null) {
			print("\t<archiver>" + structure.getArchiver() + "</archiver>\n");
		} 
		if (structure.getArchiverContact() != null) {
			print("\t<archiverContact>" + structure.getArchiverContact() 
					+ "</archiverContact>\n");
		}
		if (structure.getDataOwner() != null) {
			print("\t<dataOwner>" + structure.getDataOwner()
					+ "</dataOwner>\n");
		} else {
			throw new ModuleException("Error while exporting structure: "
					+ "data owner cannot be null");
		}
		if (structure.getDataOriginTimespan() != null) {
			print("\t<dataOriginTimespan>" + structure.getDataOriginTimespan() 
					+ "</dataOriginTimespan>\n");
		}
		if (structure.getProducerApplication() != null) {
			print("\t<producerApplication>" + Main.APP_NAME
					+ "</producerApplication>\n");
		}
		print("\t<archivalDate>" + getCurrentDate() + "</archivalDate>\n");
		print("\t<messageDigest>" + getMessageDigest() + "</messageDigest>\n");
		
		if (structure.getClientMachine() != null) {
			print("\t<clientMachine>" + structure.getClientMachine()
					+ "</clientMachine>\n");
		}
		if (structure.getProductName() != null) {
			print("\t<databaseProduct>" + structure.getProductName());
			if (structure.getProductVersion() != null) {
				print(" " + structure.getProductVersion());
			}
			print("</databaseProduct>\n");
		}
		if (structure.getUrl()!= null) {
			print("\t<connection>" + structure.getUrl()
					+ "</connection>\n");
		}
		if (structure.getDatabaseUser() != null) {
			print("\t<databaseUser>" + structure.getDatabaseUser()
					+ "</databaseUser>\n");
		}
		print("\t<schemas>\n");
		for (SchemaStructure schema : structure.getSchemas()) {
			if (schema.getTables() != null && schema.getTables().size() > 0) {
				exportSchemaStructure(schema);
			} else {
				logger.info("Schema: '" + schema.getName() + "' was not "
						+ "exported because it does not contain any table");
			}
		}
		print("\t</schemas>\n");
		
		if (structure.getUsers() != null && structure.getUsers().size() > 0) {
			print("\t<users>\n");
			for (UserStructure user : structure.getUsers()) {
				exportUserStructure(user);
			}
			print("\t</users>\n"); 
		} else {
			throw new ModuleException("Error while exporting database "
					+ "structure: users cannot be null");
		}
		
		if (structure.getRoles() != null && structure.getRoles().size() > 0) {
			print("\t<roles>\n");
			for (RoleStructure role : structure.getRoles()) {
				exportRoleStructure(role);
			}
			print("\t</roles>\n");
		}
		
		if (structure.getPrivileges() != null 
				&& structure.getPrivileges().size() > 0) {
			print("\t<privileges>\n");
			for (PrivilegeStructure schema : structure.getPrivileges()) {
				exportPrivilegeStructure(schema);
			}
			print("\t</privileges>\n");
		}
		
		
		print("</siardArchive>");
	}
	
	private void exportSchemaStructure(SchemaStructure schema) 
			throws IOException, ModuleException, UnknownTypeException {
		print("\t\t<schema>\n");
		if (schema.getName() != null) {
			print("\t\t\t<name>" + schema.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting schema structure: "
					+ "schema name cannot be null");
		}
		if (schema.getFolder() != null) {
			print("\t\t\t<folder>" + schema.getFolder() + "</folder>\n");
		} else {
			throw new ModuleException("Error while exporting schema structure: "
					+ "schema folder cannot be null");
		}
		if (schema.getDescription() != null) {
			print("\t\t\t<description>" + schema.getDescription() 
					+ "</description>\n");
		}
		
		if (schema.getTables() != null && schema.getTables().size() > 0) {
			print("\t\t\t<tables>\n");
			for (TableStructure table: schema.getTables()) {
				exportTableStructure(table);
			}
			print("\t\t\t</tables>\n");
		}
		else {
			throw new ModuleException("Error while exporting schema structure: "
					+ "schema tables cannot be null");
		}
		
		if (schema.getViews() != null && schema.getViews().size() > 0) {
			print("\t\t\t<views>\n");
			for (ViewStructure view: schema.getViews()) {
				exportViewStructure(view);
			}
			print("\t\t\t</views>\n");
		}
		
		if (schema.getRoutines() != null && schema.getRoutines().size() > 0) {
			print("\t\t\t<routines>\n");
			for (RoutineStructure routine: schema.getRoutines()) {
				exportRoutineStructure(routine);
			}
			print("\t\t\t</routines>\n");
		}
		print("\t\t</schema>\n");
	}
	

	private void exportTableStructure(TableStructure table) 
			throws IOException, ModuleException, UnknownTypeException {
		print("\t\t\t\t<table>\n");
		if (table.getName() != null) {
			print("\t\t\t\t\t<name>" + table.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting table structure: "
					+ "table name cannot be null");
		}
		if (table.getFolder() != null) {
			print("\t\t\t\t\t<folder>" + table.getFolder() + "</folder>\n");
		} else {
			throw new ModuleException("Error while exporting table structure: "
					+ "talbe folder cannot be null");
		}
		if (table.getDescription() != null) {
			print("\t\t\t\t\t<description>" + table.getDescription() 
					+ "</description>\n");
		}
		
		print("\t\t\t\t\t<columns>\n");
		for (ColumnStructure column : table.getColumns()) {
			exportColumnStructure(column);
		}
		print("\t\t\t\t\t</columns>\n");
		
		if (table.getPrimaryKey() != null) {
			print("\t\t\t\t\t<primaryKey>\n");
			exportPrimaryKey(table.getPrimaryKey());
			print("\t\t\t\t\t</primaryKey>\n");
		}
		
		if (table.getForeignKeys() != null 
				&& table.getForeignKeys().size() > 0) {			
			print("\t\t\t\t\t<foreignKeys>\n");
			for (ForeignKey foreignKey : table.getForeignKeys()) {
				exportForeignKey(foreignKey);
			}
			print("\t\t\t\t\t</foreignKeys>\n");
		}
		
		if (table.getCandidateKeys() != null 
				&& table.getCandidateKeys().size() > 0) {
			print("\t\t\t\t\t<candidateKeys>\n");
			for (CandidateKey candidateKey : table.getCandidateKeys()) {
				exportCandidateKey(candidateKey);
			}
			print("\t\t\t\t\t</candidateKeys>\n");
		}
		
		if (table.getCheckConstraints() != null 
				&& table.getCheckConstraints().size() > 0) {
			print("\t\t\t\t\t<checkConstraints>\n");
			for (CheckConstraint checkConstraint : table.getCheckConstraints()) {
				exportCheckConstraint(checkConstraint);
			}
			print("\t\t\t\t\t</checkConstraints>\n");
		}
		
		if (table.getTriggers() != null 
				&& table.getTriggers().size() > 0) {
			print("\t\t\t\t\t<triggers>\n");
			for (Trigger trigger : table.getTriggers()) {
				exportTrigger(trigger);
			}
			print("\t\t\t\t\t</triggers>\n");
		}
		
		if (table.getRows() != -1) {
			print("\t\t\t\t\t<rows>" + table.getRows() + "</rows>\n");			
		} else {
			throw new ModuleException("Error while exporting table structure: "
					+ "table rows cannot be null");
		}
		print("\t\t\t\t</table>\n");
	}
	

	private void exportColumnStructure(ColumnStructure column) 
			throws IOException, ModuleException, UnknownTypeException {
		print("\t\t\t\t\t\t<column>\n");
		if (column.getName() != null) {
			print("\t\t\t\t\t\t\t<name>" + column.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting table structure: "
					+ "column name cannot be null");
		}
		if (column.getFolder() != null) {
			print("\t\t\t\t\t\t\t<folder>" + column.getFolder() + "</folder>\n");
		}
		if (column.getType() != null) {
			print("\t\t\t\t\t\t\t<type>" + exportType(column.getType()) 
					+ "</type>\n");
		} else {
			throw new ModuleException("Error while exporting table structure: "
					+ "column type cannot be null");
		}
		if (column.getType() != null) {
			print("\t\t\t\t\t\t\t<typeOriginal>" 
		+ column.getType().getOriginalTypeName()
					+ "</typeOriginal>\n");
		}
		if (column.getDefaultValue() != null) {
			print("\t\t\t\t\t\t\t<defaultValue>" + column.getDefaultValue()
					+ "</defaultValue>\n");
		}
		if (column.isNillable() != null) {
			print("\t\t\t\t\t\t\t<nullable>" + column.isNillable()
					+ "</nullable>\n");
		}
		if (column.getDescription() != null 
				&& !column.getDescription().isEmpty()) {
			print("\t\t\t\t\t\t\t<description>" + column.getDescription()
					+ "</description>\n");
		}
		
		print("\t\t\t\t\t\t</column>\n");		
	}
	
	private String exportType(Type type) 
			throws ModuleException, IOException, UnknownTypeException {
		// TODO return correct types
		if (type instanceof SimpleTypeString) {
			String ret = null;
			SimpleTypeString stringType = (SimpleTypeString) type;
			if (stringType.getLength() > 255) {
				ret = "TEXT";
			} else 
			ret = "CHARACTER VARYING(" + stringType.getLength() + ")";
			return ret;
		}
		else if (type instanceof SimpleTypeNumericExact) {
			SimpleTypeNumericExact numExactType = (SimpleTypeNumericExact) type;
			StringBuilder sb = new StringBuilder();
			sb.append("NUMERIC(");
			sb.append(numExactType.getPrecision());
			if (numExactType.getScale() > 0) {
				sb.append(numExactType.getScale());
			}
			sb.append(")");
			return sb.toString();
		} else if (type instanceof SimpleTypeNumericApproximate) {
			SimpleTypeNumericApproximate numApproxType = 
					(SimpleTypeNumericApproximate) type;
			StringBuilder sb = new StringBuilder();
			sb.append("FLOAT");
			// FLOAT default precision is 1: returns only "FLOAT"
			if (numApproxType.getPrecision() > 1) {
				sb.append("(");
				sb.append(numApproxType.getPrecision());
				sb.append(")");
			}
			return sb.toString();
		} else if (type instanceof SimpleTypeBoolean) {
			return "BOOLEAN";
		} else if (type instanceof SimpleTypeDateTime) {
			// TODO datetime types 
			SimpleTypeDateTime dateTimeType = (SimpleTypeDateTime) type;
			if (!dateTimeType.getTimeDefined() 
					&& !dateTimeType.getTimeZoneDefined()) {
				return "DATE";
			} else {
				return "TIMESTAMP";
			}
		} else if (type instanceof SimpleTypeBinary) {
			// TODO test type BLOB
			// SimpleTypeBinary binary = (SimpleTypeBinary) type;
			return "BINARY LARGE OBJECT";
		} else if (type instanceof ComposedTypeArray) {
			throw new ModuleException("Not yet supported type: ARRAY");
		} else if (type instanceof ComposedTypeStructure) {
			throw new ModuleException("Not yet supported type: ROW");
		} else {
			throw new UnknownTypeException(type.toString());
		}
	}

	private void exportPrimaryKey(PrimaryKey primaryKey) 
			throws IOException, ModuleException {			
		if (primaryKey.getName() != null) {
			print("\t\t\t\t\t\t<name>" + primaryKey.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting primary key: "
					+ "name cannot be null");
		}
		if (primaryKey.getColumnNames() != null) {
			for (String column : primaryKey.getColumnNames()) {
				print("\t\t\t\t\t\t<column>" + column + "</column>\n");
			}
		} else {
			throw new ModuleException("Error while exporting primary key: "
					+ "column list cannot be null");
		}
		if (primaryKey.getDescription() != null) {
			print("\t\t\t\t\t\t<description>" + primaryKey.getDescription()
					+ "</description>\n");
		}
	}
	
	private void exportForeignKey(ForeignKey foreignKey) 
			throws IOException, ModuleException {
		print("\t\t\t\t\t\t<foreignKey>\n");
		if (foreignKey.getName() != null) {
			print("\t\t\t\t\t\t\t<name>" + foreignKey.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting foreign key: "
					+ "name cannot be null");
		}
		if (foreignKey.getReferencedSchema() != null) {
			print("\t\t\t\t\t\t\t<referencedSchema>" 
					+ foreignKey.getReferencedSchema() 
					+ "</referencedSchema>\n");
		}
		else {
			throw new ModuleException("Error while exporting foreign key: "
					+ "referencedSchema cannot be null");
		}
		if (foreignKey.getReferencedTable() != null) {
			print("\t\t\t\t\t\t\t<referencedTable>" 
					+ foreignKey.getReferencedTable() 
					+ "</referencedTable>\n");
		} else {
			throw new ModuleException("Error while exporting foreign key: "
					+ "referencedSchema cannot be null");
		}
		if (foreignKey.getReferences() != null 
				&& foreignKey.getReferences().size() > 0) {
			for (Reference ref : foreignKey.getReferences()) {
				print("\t\t\t\t\t\t\t<reference>\n");
				print("\t\t\t\t\t\t\t\t<column>" + ref.getColumn() 
						+ "</column>\n");
				print("\t\t\t\t\t\t\t\t<referenced>" + ref.getReferenced() 
						+ "</referenced>\n");
				print("\t\t\t\t\t\t\t</reference>\n");
			}
		} else {
			throw new ModuleException("Error while exporting foreign key: "
					+ "reference cannot be null or empty");
		}
				
		if (foreignKey.getMatchType() != null) {
			print("\t\t\t\t\t\t\t<matchType>" + foreignKey.getMatchType()
					+ "</matchType>\n");
		}		
		if (foreignKey.getDeleteAction() != null) {
			print("\t\t\t\t\t\t\t<deleteAction>" + foreignKey.getDeleteAction()
					+ "</deleteAction>\n");
		}
		if (foreignKey.getUpdateAction() != null) {
			print("\t\t\t\t\t\t\t<updateAction>" + foreignKey.getUpdateAction()
					+ "</updateAction>\n");
		}
		if (foreignKey.getDescription() != null) {
			print("\t\t\t\t\t\t\t<description>" + foreignKey.getDescription()
					+ "</description>\n");
		}
		
		print("\t\t\t\t\t\t</foreignKey>\n");
	}
	
	private void exportCandidateKey(CandidateKey candidateKey) 
			throws IOException, ModuleException {
		print("\t\t\t\t\t\t<candidateKey>\n");
		if (candidateKey.getName() != null) {
			print("\t\t\t\t\t\t\t<name>" + candidateKey.getName()
					+ "</name>\n");
		} else {
			throw new ModuleException("Error while exporting candidate key: "
					+ "candidate key name cannot be null");
		}	
		if (candidateKey.getDescription() != null) {
			print("\t\t\t\t\t\t\t<description>" + candidateKey.getDescription()
					+ "</description>\n");
		}	
		if (candidateKey.getColumns() != null 
				&& candidateKey.getColumns().size() > 0) {
			for (String column : candidateKey.getColumns()) {
				print("\t\t\t\t\t\t\t<column>" + column + "</column>\n");
			}
		} else {
			throw new ModuleException("Error while exporting candidate key: "
					+ "columns cannot be be null or empty");
		}	
		
		print("\t\t\t\t\t\t</candidateKey>\n");
		
	}

	private void exportCheckConstraint(CheckConstraint checkConstraint) 
			throws IOException, ModuleException {
		print("\t\t\t\t\t\t<checkConstraint>\n");
		if (checkConstraint.getName() != null) {
			print("\t\t\t\t\t\t\t<name>" + checkConstraint.getName()
					+ "</name>\n");
		} else {
			throw new ModuleException("Error while exporting check constraint: "
					+ "check constraint key name cannot be null");
		}	
		// TODO verify it's true, false or unknown
		if (checkConstraint.getCondition() != null) {
			print("\t\t\t\t\t\t\t<condition>" + checkConstraint.getCondition()
					+ "</condition>\n");
		} else {
			throw new ModuleException("Error while exporting candidate key: "
					+ "check constraint condition cannot be null");
		}
		if (checkConstraint.getDescription() != null) {
			print("\t\t\t\t\t\t\t<description>" 
					+ checkConstraint.getDescription() + "</description>\n");
		}			
		print("\t\t\t\t\t\t</checkConstraint>\n");				
	}
	
	private void exportTrigger(Trigger trigger) 
			throws IOException, ModuleException {
		print("\t\t\t\t\t\t<trigger>\n");
		if (trigger.getName() != null) {
			print("\t\t\t\t\t\t\t<name>" + trigger.getName()
					+ "</name>\n");
		} else {
			throw new ModuleException("Error while exporting trigger: "
					+ "trigger name key name cannot be null");
		}	
		// TODO verify it's BEFORE or AFTER
		if (trigger.getActionTime() != null) {
			print("\t\t\t\t\t\t\t<actionTime>" + trigger.getActionTime()
					+ "</actionTime>\n");
		} else {
			throw new ModuleException("Error while exporting trigger: "
					+ "trigger actionTime cannot be null");
		}
		// TODO verify it's INSERT, DELETE, UPDATE [OF <trigger column list>]
		if (trigger.getTriggerEvent() != null) {
			print("\t\t\t\t\t\t\t<triggerEvent>" + trigger.getTriggerEvent()
					+ "</triggerEvent>\n");
		} else {
			throw new ModuleException("Error while exporting trigger: "
					+ "trigger triggerEvent cannot be null");
		}
		if (trigger.getAliasList() != null) {
			print("\t\t\t\t\t\t\t<aliasList>" + trigger.getAliasList()
					+ "</aliasList>\n");
		}	
		if (trigger.getTriggeredAction() != null) {
			print("\t\t\t\t\t\t\t<triggeredAction>" + trigger.getTriggeredAction()
					+ "</triggeredAction>\n");
		} else {
			throw new ModuleException("Error while exporting trigger: "
					+ "trigger triggeredAction cannot be null");
		}
		if (trigger.getDescription() != null) {
			print("\t\t\t\t\t\t\t<description>" + trigger.getDescription()
					+ "</description>\n");
		}	
		
		print("\t\t\t\t\t\t</trigger>\n");
	}

	private void exportViewStructure(ViewStructure view) 
		throws IOException, ModuleException, UnknownTypeException {
			print("\t\t\t\t<view>\n");
			if (view.getName() != null) {
				print("\t\t\t\t\t<name>" + view.getName() + "</name>\n");
			} else {
				throw new ModuleException("Error while exporting view: "
						+ "view name cannot be null");
			}
			if (view.getQuery() != null) {
				print("\t\t\t\t\t<query>" + view.getQuery() + "</query>\n");
			}				
			if (view.getQueryOriginal() != null) {
				print("\t\t\t\t\t<queryOriginal>" + view.getQueryOriginal()
						+ "</queryOriginal>\n");
			}				
			if (view.getDescription() != null) {
				print("\t\t\t\t\t<description>" + view.getDescription()
						+ "</description>\n");
			}				
			if (view.getColumns() != null && view.getColumns().size() > 0) {
				print("\t\t\t\t\t<columns>\n");
				for (ColumnStructure column : view.getColumns()) {
					exportColumnStructure(column);
				}
				print("\t\t\t\t\t</columns>\n");
			}
			print("\t\t\t\t</view>\n");
	}

	private void exportRoutineStructure(RoutineStructure routine) 
		throws IOException, ModuleException, UnknownTypeException {
			print("\t\t\t\t<routine>\n");
			if (routine.getName() != null) {
				print("\t\t\t\t\t<name>" + routine.getName() + "</name>\n");
			} else {
				throw new ModuleException("Error while exporting routine: "
						+ "routine name cannot be null");
			}
			if (routine.getDescription() != null) {
				print("\t\t\t\t\t<description>" + routine.getDescription() 
						+ "</description>\n");
			}	
			if (routine.getSource() != null) {
				print("\t\t\t\t\t<source>" + routine.getSource() 
						+ "</source>\n");
			}	
			if (routine.getBody() != null) {
				print("\t\t\t\t\t<body>" + routine.getBody() 
						+ "</body>\n");
			}	
			if (routine.getCharacteristic() != null) {
				print("\t\t\t\t\t<caracteristic>" + routine.getCharacteristic() 
						+ "</characteristic>\n");
			}	
			if (routine.getReturnType() != null) {
				print("\t\t\t\t\t<returnType>" + routine.getReturnType() 
						+ "</returnType>\n");
			}	
			if (routine.getParameters() != null 
					&& routine.getParameters().size() > 0) {
				print("\t\t\t\t\t<parameters\n>");
				for (Parameter param : routine.getParameters()) {
					if (param.getName() != null) {
						print("\t\t\t\t\t\t<name>" + param.getName() 
								+ "</name>\n");
					} else {
						throw new ModuleException("Error while exporting "
								+ "routine parameters: "
								+ "parameter name cannot be null");
					}
					if (param.getMode() != null) {
						print("\t\t\t\t\t\t<mode>" + param.getMode() 
								+ "</mode>\n");
					} else {
						throw new ModuleException("Error while exporting "
								+ "routine parameters: "
								+ "parameter mode cannot be null");
					}
					if (param.getType() != null) {
						print("\t\t\t\t\t\t<type>" + exportType(param.getType()) 
								+ "</type>\n");
					} else {
						throw new ModuleException("Error while exporting "
								+ "routine parameters: "
								+ "parameter type cannot be null");
					}
					if (param.getType() != null) {
						print("\t\t\t\t\t\t<typeOriginal>" 
								+ param.getType().getOriginalTypeName()
								+ "</typeOriginal>\n");
					}
					if (param.getDescription() != null) {
						print("\t\t\t\t\t\t<description>" 
								+ param.getDescription() + "</description>\n");
					}
				}
				print("\t\t\t\t\t</parameters>\n");
			}	
		
			print("\t\t\t\t</routine>\n");
	}

	private void exportUserStructure(UserStructure user) 
			throws IOException, ModuleException {
		print("\t\t<user>\n");
		if (user.getName() != null) {
			print("\t\t\t<name>" + user.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "user name cannot be null");
		}			
		if (user.getDescription() != null) {
			print("\t\t\t<description>" + user.getName() + "</description>\n");
		}
		print("\t\t</user>\n");		
	}

	private void exportRoleStructure(RoleStructure role) 
			throws IOException, ModuleException {
		print("\t\t<role>\n");
		if (role.getName() != null) {
			print("\t\t\t<name>" + role.getName() + "</name>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "user name cannot be null");
		}	
		if (role.getAdmin() != null) {
			print("\t\t\t<admin>" + role.getAdmin() + "</admin>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "role admin cannot be null");
		}	
		if (role.getDescription() != null) {
			print("\t\t\t<description>" + role.getName() + "</description\n");
		}
		print("\t\t</role>\n");
	}
	
	private void exportPrivilegeStructure(PrivilegeStructure privilege) 
			throws ModuleException, IOException {
		print("\t\t<privilege>\n");
		if (privilege.getType() != null) {
			print("\t\t\t<type>" + privilege.getType() + "</type>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "privilege type cannot be null");
		}	
		if (privilege.getObject() != null) {
			print("\t\t\t<object>" + privilege.getObject() + "</object>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "privilege object cannot be null");
		}	
		if (privilege.getGrantor() != null) {
			print("\t\t\t<grantor>" + privilege.getGrantor() + "</grantor>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "privilege grantor cannot be null");
		}	
		if (privilege.getGrantee() != null) {
			print("\t\t\t<grantee>" + privilege.getGrantee() + "</grantee>\n");
		} else {
			throw new ModuleException("Error while exporting users structure: "
					+ "privilege grantee cannot be null");
		}	
		if (privilege.getOption() != null) {
			print("\t\t\t<option>" + privilege.getOption() + "</option>\n");
		} 
		if (privilege.getDescription() != null) {
			print("\t\t\t<description>" + privilege.getDescription() 
					+ "</description>\n");
		} 		
		print("\t\t</privilege>\n");
	}

	private void exportDataOpenTable() throws IOException {
		print("<?xml version=\"1.0\" encoding=\"" + ENCODING + "\"?>\n");
		print("<?xml-stylesheet type=\"text/xsl\" href=\"metadata.xsl\"?>");
		// TODO complete xml header
		print("<table xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");		
	}
	
	private void exportDataCloseTable() throws IOException {
		print("</table>");
	}
	
	private void exportRowData(Row row) throws IOException {
		print("<row>");
		int index = 0;
		for (Cell cell : row.getCells()) {
			index++;
			print("<c" + index + ">");
			exportCellData(cell);
			print("</c" + index + ">");
		}
		print("</row>\n");		
	}
	
	
	// TODO add support to other cell types
	private void exportCellData(Cell cell) throws IOException {
		//print("\t\t\t");
		if (cell instanceof SimpleCell) {
			SimpleCell simple = (SimpleCell) cell;
			if (simple.getSimpledata() != null) {
				// print(encode(simple.getSimpledata()));
				print(SIARDHelper.encode(simple.getSimpledata()));
			} else {
				print("");
			}
		}
		//print("\n");
	}

	/**
	 * Get current date and time
	 * 
	 * @return the date in ISO 8601 format, with no milliseconds
	 */
	private String getCurrentDate() {
		return new SimpleDateFormat("yyyy-MM-dd").format(new Date());		
	}
	
	private String getMessageDigest() {
		String messageDigest = "MD5DefaultMD";
		// TODO calc message digest
		return messageDigest;
	}
	
	private void print(String s) throws IOException {
		zipOut.write(s.getBytes());
	}
}
