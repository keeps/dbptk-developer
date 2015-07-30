package com.databasepreservation.modules.siard.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.path.PathStrategy;
import com.databasepreservation.modules.siard.write.WriteStrategy;

/**
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 *
 */
public interface MetadataStrategy {
	public void output(DatabaseStructure database, PathStrategy paths, WriteStrategy writer)
			throws ModuleException;
}
