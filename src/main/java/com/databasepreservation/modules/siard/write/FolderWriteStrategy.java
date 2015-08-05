package com.databasepreservation.modules.siard.write;

import com.databasepreservation.model.exception.ModuleException;
import com.sun.org.apache.xpath.internal.operations.Mod;
import org.apache.commons.io.IOUtils;
import sun.security.pkcs11.Secmod;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class FolderWriteStrategy implements WriteStrategy {
	@Override
	public OutputStream createOutputStream(OutputContainer container, String path) throws ModuleException {
		Path filepath = container.getPath().resolve(path);

		if( !Files.exists(filepath) ){
			try {
				if( !Files.exists(filepath.getParent()) ){
					Files.createDirectories(filepath.getParent());
				}
				Files.createFile(filepath);
			} catch (IOException e) {
				throw new ModuleException("Error while creating the file: " + filepath.toString(),e);
			}
		}

		try {
			return Files.newOutputStream(filepath);
		} catch (IOException e) {
			throw new ModuleException("Error while getting the file: " + filepath.toString(),e);
		}
	}

	@Override
	public boolean supportsSimultaneousWriting() {
		return true;
	}

	@Override
	public void finish(OutputContainer baseContainer) throws ModuleException {
		// nothing to do
	}

	@Override
	public void setup(OutputContainer baseContainer) throws ModuleException {
		// nothing to do
	}
}
