package com.databasepreservation.modules.siard.write;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;

public interface OutputContainer {
	public void writeFile(String path, InputStream dataSource)
			throws ModuleException;
}
