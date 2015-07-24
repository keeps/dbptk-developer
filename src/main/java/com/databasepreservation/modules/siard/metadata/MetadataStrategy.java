package com.databasepreservation.modules.siard.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;

/**
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 *
 */
public interface MetadataStrategy {
	public void output(DatabaseStructure database)
			throws ModuleException;
}
