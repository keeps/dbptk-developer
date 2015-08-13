package com.databasepreservation.modules.siard.common;

import java.io.InputStream;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class LargeObject {
	private final InputStream datasource;
	private final String path;

	public LargeObject(InputStream datasource, String path) {
		this.datasource = datasource;
		this.path = path;
	}

	public InputStream getDatasource() {
		return datasource;
	}

	public String getPath() {
		return path;
	}
}
