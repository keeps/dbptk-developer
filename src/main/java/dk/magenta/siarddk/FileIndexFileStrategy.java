/**
 * The methods should be called in this order from the SIARDDKMetadataExportStrategy
 * 1) getWriter
 * 2) addFile
 * 3) generateXML
 */
package dk.magenta.siarddk;

import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

public class FileIndexFileStrategy implements IndexFileStrategy {

	private WriteStrategy writeStrategy;
	
	public FileIndexFileStrategy(WriteStrategy writeStrategy) {
		this.writeStrategy = writeStrategy;		
	}
	
	@Override
	public Object generateXML(DatabaseStructure dbStructure)
			throws ModuleException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public OutputStream getWriter(SIARDArchiveContainer outputContainer, String path) throws ModuleException {
		
		OutputStream writerFromWriteStrategy = writeStrategy.createOutputStream(outputContainer, path);
		DigestOutputStream writer = null;
		try {
			MessageDigest digest = MessageDigest.getInstance("MD5");
			writer = new DigestOutputStream(writerFromWriteStrategy, digest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return writer;
	}
	
	/**
	 * Adds file to archive.
	 * @param outputContainer
	 * @param path
	 * @return md5sum of file
	 */
	public String addFile(SIARDArchiveContainer outputContainer, String path) {
		// TO-DO:
		// Make container that the file should be added to
		// Calculate md5sum
		
		return null;
	}

}
