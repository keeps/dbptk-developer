package com.databasepreservation.modules.siard.write;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;

public interface WriteStrategy {
	public void write(OutputContainer container, String path, InputStream dataSource)
			throws ModuleException;
}
