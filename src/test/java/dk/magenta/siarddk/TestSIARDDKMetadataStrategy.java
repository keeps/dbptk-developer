package dk.magenta.siarddk;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import dk.magenta.common.MetadataStrategy;

public class TestSIARDDKMetadataStrategy {

	//TO-DO: Test more thoroughly that dbName is correct regular expression
	
	private DatabaseStructure dbStructure;
	private MetadataStrategy siarddkMetadataStrategy;
	
	@Before
	public void setUp() {
		dbStructure = new DatabaseStructure();
		siarddkMetadataStrategy = new SIARDDKMetadataStrategy();
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameNull() throws ModuleException {
		dbStructure.setName(null);
		siarddkMetadataStrategy.generateMetaData(dbStructure);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameBeginsWithNumber() throws ModuleException {
		dbStructure.setName("1test");
		siarddkMetadataStrategy.generateMetaData(dbStructure);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameTooShort() throws ModuleException {
		dbStructure.setName("");
		siarddkMetadataStrategy.generateMetaData(dbStructure);
	}

	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenDbNameTooLong() throws ModuleException {
		dbStructure.setName("ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt");
		siarddkMetadataStrategy.generateMetaData(dbStructure);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenThereAreNoSchemas() throws ModuleException {
		dbStructure.setName("test");
		dbStructure.setSchemas(null);
		siarddkMetadataStrategy.generateMetaData(dbStructure);
	}
	
	@Test(expected=ModuleException.class)
	public void shouldThrowExceptionWhenThereAreNoTables() throws ModuleException {
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(null);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);
		siarddkMetadataStrategy.generateMetaData(dbStructure);
	}
	
	@Ignore
	@Test
	public void fail() {
		assertTrue(false);
	}
	
}
