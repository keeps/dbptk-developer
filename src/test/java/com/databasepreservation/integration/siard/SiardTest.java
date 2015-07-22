package com.databasepreservation.integration.siard;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import org.w3c.util.DateParser;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.in.SIARDImportModule;
import com.databasepreservation.modules.siard.out.SIARDExportModule;

@Test(groups={"siard-roundtrip"})
public class SiardTest {

	private static final Logger logger = Logger.getLogger(SiardTest.class);

	@Test
	public void HelloWorld() throws ModuleException, IOException, UnknownTypeException, InvalidDataException{
		// siard module
		Path tmpFile = Files.createTempFile("roundtripSIARD_", ".zip");
		SIARDExportModule exporter = new SIARDExportModule(tmpFile.toFile(), false);

		// create a dataset
		int defaultTransactionIsolationLevel = -1;
		Boolean supportsANSI92EntryLevelSQL = true;
		Boolean supportsANSI92IntermediateSQL = true;
		Boolean supportsANSI92FullSQL = true;
		Boolean supportsCoreSQLGrammar = true;
		ArrayList<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
		ArrayList<TableStructure> tables = new ArrayList<TableStructure>();
		ArrayList<ColumnStructure> columns = new ArrayList<ColumnStructure>();
		columns.add(new ColumnStructure("colid", "colname", new SimpleTypeBoolean(), true, "column description", "1", false));
		tables.add(new TableStructure("schemaName.tablename", "tablename", "table description", columns, null, null, null, null, null, 0));
		schemas.add(new SchemaStructure("schemaName", "schemaDescription", 1,
				tables, new ArrayList<ViewStructure>(), new ArrayList<RoutineStructure>()));
		ArrayList<UserStructure> users = new ArrayList<UserStructure>();
		users.add(new UserStructure("testUser", "TestUser description"));
		ArrayList<RoleStructure> roles = new ArrayList<RoleStructure>();
		ArrayList<PrivilegeStructure> privileges = new ArrayList<PrivilegeStructure>();
		DatabaseStructure dbStructure = new DatabaseStructure(
				"name", "description", "archiver", "archiverContact", "dataOwner",
				"dataOriginTimespan", "producerApplication", "creationDate",
				"messageDigest", "clientMachine", "productName", "productVersion",
				"databaseUser", defaultTransactionIsolationLevel, "extraNameCharacters",
				"stringFunctions", "systemFunctions", "timeDateFunctions", "url",
				supportsANSI92EntryLevelSQL, supportsANSI92IntermediateSQL,
				supportsANSI92FullSQL, supportsCoreSQLGrammar, schemas,
				users, roles, privileges);

		dbStructure.setArchivalDate(DateParser.getIsoDate(new Date()));
		Map<String, List<Row>> tableRows = new HashMap<String, List<Row>>();
		ArrayList<Cell> cells = new ArrayList<Cell>();
		cells.add(new SimpleCell("idwtf", "1"));
		ArrayList<Row> rows = new ArrayList<Row>();
		rows.add(new Row(0, cells));
		tableRows.put("schemaName.tablename", rows);

		// behaviour
		logger.debug("initializing database");
		exporter.initDatabase();
		exporter.setIgnoredSchemas(new HashSet<String>());
		logger.info("STARTED: Getting the database structure.");
		exporter.handleStructure(dbStructure);
		logger.info("FINISHED: Getting the database structure.");
		logger.debug("db struct: " + dbStructure.toString());
		for (SchemaStructure thisschema : dbStructure.getSchemas()) {
			for (TableStructure thistable : thisschema.getTables()) {
				logger.info("STARTED: Getting data of table: " + thistable.getId());
				thistable.setSchema(thisschema);
				exporter.handleDataOpenTable(thisschema.getName(),thistable.getId());
				int nRows = 0;
				Iterator<Row> rowsIterator = tableRows.get(thistable.getId()).iterator();
				while (rowsIterator.hasNext()) {
					exporter.handleDataRow(rowsIterator.next());
					nRows++;
				}
				logger.info("Total of " + nRows + " row(s) processed");
				exporter.handleDataCloseTable(thisschema.getName(),thistable.getId());
				logger.info("FINISHED: Getting data of table: " + thistable.getId());
			}
		}
		logger.debug("finishing database");
		exporter.finishDatabase();

		logger.debug("done");
		logger.debug("getting the data back from SIARD");

        //MyClass myInstance = mock(MyClass.class);
		DatabaseHandler mocked = Mockito.mock(DatabaseHandler.class);

		SIARDImportModule importer = new SIARDImportModule(tmpFile.toFile());

		ArgumentCaptor<DatabaseStructure> dbStructureCaptor = ArgumentCaptor.forClass(DatabaseStructure.class);

		importer.getDatabase(mocked);

		Mockito.verify(mocked).handleStructure(dbStructureCaptor.capture());



		DatabaseStructure x = dbStructureCaptor.getValue();
		assert dbStructure.equals(x) : "The final structure (from SIARD) differs from the original structure";


		//assertEquals("John", argument.getValue().getName());
		//Assert.assertSomething(my.methodUnderTest());


	}

}
