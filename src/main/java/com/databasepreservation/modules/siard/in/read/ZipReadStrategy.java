package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipReadStrategy implements ReadStrategy {
	public ZipFile zipFile;

	@Override
	public InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException {
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
	public void finish(SIARDArchiveContainer container) throws ModuleException {
		try {
			zipFile.close();
		} catch (IOException e) {
			throw new ModuleException("Could not close zip file", e);
		}
	}

	@Override
	public void setup(SIARDArchiveContainer container) throws ModuleException {
		try {
			zipFile = new ZipFile(container.getPath().toAbsolutePath().toString());
		} catch (IOException e) {
			throw new ModuleException(
					String.format("Could not open zip file \"%s\"", container.getPath().toAbsolutePath().toString()),
					e);
		}
	}

	@Override
	public List<String> listFiles(SIARDArchiveContainer container, Path baseDirectory) throws ModuleException {
		List<String> list = new ArrayList<String>();
		Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();

		while (entries.hasMoreElements()){
			String name = entries.nextElement().getName();

			if(baseDirectory != null) {
				Path file = Paths.get(name);
				if(file.startsWith(baseDirectory)){
					list.add(name);
				}
			}else{
				list.add(name);
			}
		}
		return list;
	}
}
