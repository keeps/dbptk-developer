package dk.magenta.siarddk;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;

import dk.magenta.common.StandardSIARDMarshaller;

public class TestSIARDDKMetadataExportStrategy {

	@Test
	public void testMarshaller() throws ModuleException {
		MetadataExportStrategy metadataExportStrategy = new SIARDDKMetadataExportStrategy(new FolderWriteStrategy(), new StandardSIARDMarshaller());
		metadataExportStrategy.writeMetadataXML(null, null);
	}
	
	@Ignore
	@Test
	public void fail() {
		assertTrue(false);
	}
	
}
