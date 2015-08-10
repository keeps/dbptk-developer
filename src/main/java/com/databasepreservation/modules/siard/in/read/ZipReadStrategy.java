package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.out.write.OutputContainer;
import jdk.internal.util.xml.impl.Input;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.ZipEntry;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipReadStrategy implements ReadStrategy {
	public ZipFile zipFile;

	@Override
	public InputStream createInputStream(OutputContainer container, String path) throws ModuleException {
		InputStream stream = null;
		try {
			ZipArchiveEntry entry = zipFile.getEntry(path);
			if (entry == null) {
				throw new ModuleException(String.format("File \"%s\" is missing in container", path));
			}
			stream = zipFile.getInputStream(entry);
		} catch (IOException e) {
			throw new ModuleException(String.format("Error while accessing file \"%s\" in container", path), e);
		}
		return stream;
	}

	@Override
	public boolean isSimultaneousReadingSupported() {
		return true;
	}

	@Override
	public void finish(OutputContainer container) throws ModuleException {
		try {
			zipFile.close();
		} catch (IOException e) {
			throw new ModuleException("Could not close zip file", e);
		}
	}

	@Override
	public void setup(OutputContainer container) throws ModuleException {
		try {
			zipFile = new ZipFile(container.getPath().toAbsolutePath().toString());
		} catch (IOException e) {
			throw new ModuleException(
					String.format("Could not open zip file \"%s\"", container.getPath().toAbsolutePath().toString()),
					e);
		}
	}

	@Override
	public List<String> listFiles(OutputContainer container, String directory) throws ModuleException {
		List<String> list = new ArrayList<String>();
		Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();

		// TODO: return only files from the specified directory
		while (entries.hasMoreElements()){
			list.add(entries.nextElement().getName());
			System.out.println("entries.nextElement().getName(): " + entries.nextElement().getName()); //TODO: debug, remove this
		}
		return list;
	}
}
