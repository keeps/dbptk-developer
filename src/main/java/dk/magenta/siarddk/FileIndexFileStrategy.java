/**
 * The methods should be called in this order from the SIARDDKMetadataExportStrategy
 * 1) getWriter
 * 2) addFile (should not be called until writer obtained from the above is closed)
 * 3) generateXML
 */
package dk.magenta.siarddk;

import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.siarddk.fileindex.FileIndexType;

public class FileIndexFileStrategy implements IndexFileStrategy {

	private WriteStrategy writeStrategy;
	private MessageDigest messageDigest;
	private Map<String, String> md5sums;
	
	public FileIndexFileStrategy(WriteStrategy writeStrategy) {
		this.writeStrategy = writeStrategy;
		md5sums = new HashMap<String, String>(); 
	}
	
	@Override
	public Object generateXML(DatabaseStructure dbStructure)
			throws ModuleException {

		FileIndexType fileIndexType = new FileIndexType();
		
		for (Map.Entry<String, String> entry : md5sums.entrySet()) {
			
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		
		return null;
	}
	
	public OutputStream getWriter(SIARDArchiveContainer outputContainer, String path) throws ModuleException {
		
		OutputStream writerFromWriteStrategy = writeStrategy.createOutputStream(outputContainer, path);
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return new DigestOutputStream(writerFromWriteStrategy, messageDigest);
	}
	
	/**
	 * Adds file to archive.
	 * @param path The path in the outputContainer
	 * @return md5sum of file
	 * Pre-condition: writer to calculate md5sum from should be finished and closed.
	 */
	public String addFile(String path) {
		// Calculate md5sum
		byte[] digest = messageDigest.digest();
		String md5sum = DatatypeConverter.printHexBinary(digest).toLowerCase();

		// Add file to map
		md5sums.put(path, md5sum);
		
		return md5sum;
	}
	
//	public void print() {
//		md5sums.
//		for (String key : md5sums) {
//			System.out.println(key + " " + md5sums.get(key));
//		}
//	}

}
