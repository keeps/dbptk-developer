package dk.magenta.siarddk;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;


public class TestSIARDDKMetadataStrategy {

	//TO-DO: Test more thoroughly that dbName is correct regular expression
	
	private DatabaseStructure dbStructure;
	private SIARDDKMetadataStrategy siarddkMetadataStrategy;
	
	@Before
	public void setUp() {
		dbStructure = new DatabaseStructure();
		siarddkMetadataStrategy = new SIARDDKMetadataStrategy(dbStructure);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameNull() throws ModuleException {
		dbStructure.setName(null);
		siarddkMetadataStrategy.writeMetadataXML(null);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameBeginsWithNumber() throws ModuleException {
		dbStructure.setName("1test");
		siarddkMetadataStrategy.writeMetadataXML(null);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameTooShort() throws ModuleException {
		dbStructure.setName("");
		siarddkMetadataStrategy.writeMetadataXML(null);
	}

	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameTooLong() throws ModuleException {
		dbStructure.setName("ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt");
		siarddkMetadataStrategy.writeMetadataXML(null);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenThereAreNoSchemas() throws ModuleException {
		dbStructure.setName("test");
		dbStructure.setSchemas(null);
		siarddkMetadataStrategy.writeMetadataXML(null);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenThereAreNoTables() throws ModuleException {
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(null);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);
		siarddkMetadataStrategy.writeMetadataXML(null);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenTypeIsUnknown() throws ModuleException {
		siarddkMetadataStrategy.validateInput("unknown", "");
	}
	
	//////////////////////////// Testing validateInput() /////////////////////////
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenInputBeginsWithNumber() throws ModuleException {
		siarddkMetadataStrategy.validateInput("SQLIdentifier", "1test");
	}

	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenInputTooShort() throws ModuleException {
		siarddkMetadataStrategy.validateInput("SQLIdentifier", "");
	}

	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenInputTooLong() throws ModuleException {
		siarddkMetadataStrategy.validateInput("SQLIdentifier", "ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt");
	}
	
	@Test
	public void shouldReturnTrueIfSQLIdentifierInputIsOk() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQLIdentifier", "inputOK"));
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenSQL1999DataTypeIncorrect() throws ModuleException {
		siarddkMetadataStrategy.validateInput("SQL1999DataType", "unknown");
	}
	
	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsCharacter() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "character(20)"));
	}
	
	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsChar() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "char ( 20)"));
	}
	
	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsCharacterVarying() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "CHARACTER VARYING( 20)"));
	}
	
	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsCharVarying() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "CHAR VARYING (20)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsVarchar() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "varchar(2)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNationalCharacter() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "national character (2)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNationalChar() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "national char( 2)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNchar() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "NCHAR(2989)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNationalCharacterVarying() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "national character varying (2989)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNationalCharVarying() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "national char varying ( 1)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNCharVarying() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "nchar varying(1)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsNumeric() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "numeric (1, 123)"));
	}

	@Test
	public void shouldReturnTrueWhenSQL1999DataTypeIsTime() throws ModuleException {
		assertTrue(siarddkMetadataStrategy.validateInput("SQL1999DataType", "time (232) WITH TIME ZONE"));
	}

	
	///////////////////////////////////////////////////////////////////////////////////////
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionIfTableNameInvalid() throws ModuleException {
		TableStructure tableStructure = new TableStructure();
		tableStructure.setName("1test");
		List<TableStructure> tableList = new ArrayList<TableStructure>();
		tableList.add(tableStructure);
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(tableList);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);
		siarddkMetadataStrategy.writeMetadataXML(null);
	}
	
	@Ignore
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenFolderNameInvalid() throws ModuleException {
		
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenColumnsNameNotValid() throws ModuleException {
		ColumnStructure columnStructure = new ColumnStructure();
		columnStructure.setName("1test");
		List<ColumnStructure> columnList = new ArrayList<ColumnStructure>();
		columnList.add(columnStructure);
		TableStructure tableStructure = new TableStructure();
		tableStructure.setName("table1");
		tableStructure.setColumns(columnList);
		List<TableStructure> tableList = new ArrayList<TableStructure>();
		tableList.add(tableStructure);
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(tableList);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);
		siarddkMetadataStrategy.writeMetadataXML(null);
		
	}

	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenColumnsTypeNotValid() throws ModuleException {
		ColumnStructure columnStructure = new ColumnStructure();
		columnStructure.setName("c1");
		Type type = new SimpleTypeString(20, true);
		type.setSql99TypeName("unknown");
		columnStructure.setType(type);
		List<ColumnStructure> columnList = new ArrayList<ColumnStructure>();
		columnList.add(columnStructure);
		TableStructure tableStructure = new TableStructure();
		tableStructure.setName("table1");
		tableStructure.setColumns(columnList);
		List<TableStructure> tableList = new ArrayList<TableStructure>();
		tableList.add(tableStructure);
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(tableList);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);
		siarddkMetadataStrategy.writeMetadataXML(null);
	}

	
	@Ignore
	@Test
	public void fail() {
		assertTrue(false);
	}
	
}
