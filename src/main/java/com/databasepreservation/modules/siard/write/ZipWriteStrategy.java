package com.databasepreservation.modules.siard.write;

import com.databasepreservation.model.exception.ModuleException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipWriteStrategy implements WriteStrategy {
	public enum CompressionMethod {
		DEFLATE, STORE
	}
	private final CompressionMethod compressionMethod;

	public ZipWriteStrategy(CompressionMethod compressionMethod){
		this.compressionMethod = compressionMethod;
	}

	@Override
	public OutputStream createOutputStream(OutputContainer container, String path) throws ModuleException {
		//TODO: use container to get the path to the zip, then use path to get the relative path to the file inside the zip
		return null;
	}
}
