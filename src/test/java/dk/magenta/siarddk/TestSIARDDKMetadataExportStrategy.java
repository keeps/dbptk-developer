package dk.magenta.siarddk;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;

public class TestSIARDDKMetadataExportStrategy {

	@Test
	public void testMarshaller() throws ModuleException {
		MetadataExportStrategy metadataExportStrategy = new SIARDDKMetadataExportStrategy(null);
		metadataExportStrategy.writeMetadataXML(null, null);
	}
	
	@Ignore
	@Test
	public void fail() {
		assertTrue(false);
	}
	
}
