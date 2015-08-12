package dk.magenta.siarddk;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;

public class TestSIARDDKContentExportPathStrategy {

	private ContentPathExportStrategy c;
	
	@Before
	public void setUp() {
		c = new SIARDDKContentExportPathStrategy();
	}
	
	@Test
	public void shouldReturnTable7WhenIndex7() {
		assertEquals("table7", c.getTableFolderName(7));
	}
	
	@Test
	public void shouldReturnCorrectTableXmlFilePath() {
		assertEquals("Tables/table7/table7.xml", c.getTableXmlFilePath(0, 7));
	}
	
	@Ignore
	@Test
	public void fail() {
		assertTrue(false);
	}
	
}
