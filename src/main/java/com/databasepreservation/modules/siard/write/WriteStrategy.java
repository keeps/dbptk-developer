package com.databasepreservation.modules.siard.write;

import java.io.*;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface WriteStrategy {
	class Utils{
		public static Writer getWriterFromOutputStream(OutputStream os){
			return new BufferedWriter(new OutputStreamWriter(os));
		}
	}

	/**
	 * Writer out = new BufferedWriter(new OutputStreamWriter(createOutputStream(...)));
	 */
	OutputStream createOutputStream(OutputContainer container, String path) throws ModuleException;
}
